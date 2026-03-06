# src/infrastructure/excel.py

# 1. Imports da Biblioteca Padrão
import os
import time
import re
from typing import List, Dict, Optional, Tuple, Any

# 2. Imports de Terceiros
import pandas as pd

# 3. Imports da Aplicação (Infraestrutura)
from src.infrastructure.logging import logger
from src.config import CONFIG

# 4. Imports do Domínio (Regras de Negócio)
from src.domain import DataSanitizer, AUDITOR

# ==============================================================================
# SERVIÇO: PROCESSADOR DE EXCEL (VERSÃO ELITE - AUTO-CORREÇÃO)
# ==============================================================================

class ExcelProcessor:
    def __init__(self, file_path: str, vendedores_cache: dict): 
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.vendedores_cache = vendedores_cache
        
        # --- [NOVO] MAPA REVERSO DE INTELIGÊNCIA ---
        # Cria um índice { 'CODIGO_OMIE_STR': {'nome': 'NOME', 'id': 'UUID'} }
        # Isso permite que o sistema "saiba" quem é o dono apenas pelo código.
        self.mapa_proprietarios = {}
        for nome, dados in self.vendedores_cache.items():
            cod = dados.get('codigo_omie')
            if cod:
                try:
                    # Normaliza para string sem decimais (ex: 123.0 -> "123")
                    cod_str = str(int(float(cod))).strip()
                    self.mapa_proprietarios[cod_str] = {
                        'nome': nome,
                        'id': dados['id']
                    }
                except:
                    continue
        # -------------------------------------------

        # Extrai a identidade da pasta (Nível Avó)
        try:
            path_obj = os.path.dirname(os.path.dirname(file_path)) 
            self.nome_pasta_vendedor = os.path.basename(path_obj).strip().upper()
        except:
            self.nome_pasta_vendedor = "DESCONHECIDO"

    def wait_for_lock_release(self, timeout: int = 25) -> bool:
        attempts = 0
        while attempts < timeout:
            try:
                with open(self.file_path, 'rb'):
                    time.sleep(0.8)
                    return True
            except (IOError, OSError):
                time.sleep(1)
                attempts += 1
                if attempts % 5 == 0:
                    logger.debug(f"⏳ [LOCK] Arquivo '{self.file_name}' ocupado ({attempts}/{timeout})...")
        return False

    def parse(self) -> Optional[Tuple[str, List[Dict]]]:
        try:
            # Engine openpyxl para máxima compatibilidade
            with pd.ExcelFile(self.file_path, engine='openpyxl') as xl:
                
                # 1. VALIDAÇÃO DE CABEÇALHO
                header = pd.read_excel(xl, header=None, nrows=1)
                if header.empty: return None
                
                ref_text = str(header.iloc[0, 0]).upper()
                match = re.search(r'(\w+)/(\d{4})', ref_text)
                
                if not match:
                    logger.warning(f"🚫 [PARSER] Cabeçalho inválido em: {self.file_name}")
                    return None

                mes_nome, ano_ref = match.group(1), int(match.group(2))
                mes_num = self._get_month_number(mes_nome)

                # Validação de Mês Alvo (.env)
                if CONFIG.TARGET_MONTH and CONFIG.TARGET_YEAR:
                    if mes_num != CONFIG.TARGET_MONTH or ano_ref != CONFIG.TARGET_YEAR:
                        return None 

                # Warm-up do Auditor
                try:
                    logger.info(f"🕵️ [AUDITOR] Carregando Omie para {mes_num:02d}/{ano_ref}...")
                    AUDITOR.prepare_context(mes_num, ano_ref)
                except Exception as e:
                    logger.error(f"⚠️ [AUDITOR] Falha ao conectar Omie: {e}")

                # 2. IDENTIFICAÇÃO DO VENDEDOR (ARQUIVO)
                meta = pd.read_excel(xl, header=None, skiprows=5, nrows=1)
                if meta.empty: return None
                
                vendedor_arquivo = str(meta.iloc[0, 0]).strip().upper()
                v_arq_norm = DataSanitizer.normalize_name(vendedor_arquivo)
                v_pasta_norm = DataSanitizer.normalize_name(self.nome_pasta_vendedor)

                # === BLINDAGEM DE IDENTIDADE ===
                # Se o vendedor não existe no banco, aborta.
                if v_arq_norm not in self.vendedores_cache:
                    logger.warning(f"🚫 [SEGURANÇA] Vendedor '{vendedor_arquivo}' desconhecido no Banco.")
                    return None

                # Se o arquivo está na pasta errada, aborta (anti-fraude básica).
                if v_arq_norm != v_pasta_norm:
                    logger.critical(f"🚨 [FRAUDE] Pasta: '{self.nome_pasta_vendedor}' vs Arquivo: '{vendedor_arquivo}'")
                    return None
                
                # 3. PROCESSAMENTO INTELIGENTE
                df = pd.read_excel(xl, skiprows=6)
                if df.empty or len(df.columns) <= CONFIG.COL_PV: return None
                df = df.dropna(subset=[df.columns[CONFIG.COL_PV]]) 
                
                vendas = []
                
                # Dados do "Dono do Arquivo" (padrão)
                dados_vendedor_origem = self.vendedores_cache[v_arq_norm]
                
                for i, row in df.iterrows():
                    # Processa a linha com inteligência de redirecionamento
                    venda = self._process_row(row, i + 8, vendedor_arquivo, dados_vendedor_origem, mes_num, ano_ref)
                    if venda:
                        vendas.append(venda)
                        
                return vendedor_arquivo, vendas

        except Exception as e:
            logger.error(f"💥 [PARSER] Erro crítico em {self.file_name}: {str(e)}")
            return None

    def _process_row(self, row: pd.Series, num_linha: int, vendedor_origem_nome: str, dados_origem: dict, mes_ref: int, ano_ref: int) -> Optional[Dict]:
        try:
            # --- Validações Básicas ---
            data_raw = row.iloc[CONFIG.COL_DATA]
            if pd.isna(data_raw): return None
            data_dt = pd.to_datetime(data_raw, errors='coerce')
            if pd.isna(data_dt): return None
            
            if data_dt.month != mes_ref or data_dt.year != ano_ref:
                # logger.warning(f"⚠️ [LINHA {num_linha}] Data fora do mês ({data_dt}).")
                return None

            pv_raw = str(row.iloc[CONFIG.COL_PV]).strip().split('.')[0] 
            if not pv_raw.isdigit() or pv_raw == "0":
                logger.warning(f"⚠️ [LINHA {num_linha}] PV inválido: {pv_raw}")
                return None

            valor_ped = DataSanitizer.clean_numeric(row.iloc[CONFIG.COL_VALOR_PEDIDO])
            valor_pend = DataSanitizer.clean_numeric(row.iloc[CONFIG.COL_VALOR_PENDENTE])
            valor_comi = DataSanitizer.clean_numeric(row.iloc[CONFIG.COL_VALOR_COMISSAO])
            if valor_ped <= 0: return None

            cliente = str(row.iloc[CONFIG.COL_CLIENTE]).strip().upper()[:100]

            # ==================================================================
            # 🛡️ SISTEMA DE AUTO-CORREÇÃO DE TITULARIDADE (NÍVEL ELITE)
            # ==================================================================
            
            # 1. Consulta a Verdade na Omie
            audit = AUDITOR.auditar_linha(pv_raw, valor_ped)
            status_auditoria = audit["status_auditoria"]
            cod_omie_real = str(audit.get("codigo_vendedor", "")).strip()

            # Variáveis Finais (Padrão = Vendedor da Planilha)
            vendedor_final_nome = vendedor_origem_nome
            vendedor_final_id = dados_origem['id']
            tipo_registro = "CORRENTE"

            # 2. Análise Forense
            # Se o pedido EXISTE na Omie e tem um código de vendedor...
            if status_auditoria not in ["NAO_ENCONTRADO_OMIE", "REPROVADO"] and cod_omie_real:
                
                # Quem é o dono segundo a Omie?
                proprietario_real = self.mapa_proprietarios.get(cod_omie_real)
                
                if proprietario_real:
                    # Se o dono real for DIFERENTE de quem mandou a planilha
                    if proprietario_real['nome'] != vendedor_origem_nome:
                        
                        # AÇÃO: REDIRECIONAMENTO AUTOMÁTICO
                        logger.warning(f"♻️ [AUTO-FIX] PV {pv_raw} movido de {vendedor_origem_nome} para {proprietario_real['nome']} (Dono Verdadeiro).")
                        
                        vendedor_final_nome = proprietario_real['nome']
                        vendedor_final_id = proprietario_real['id']
                        status_auditoria = "REDIRECIONADO_AUTO" # Marca para você saber
                        tipo_registro = "RECUPERADO"
                
                elif dados_origem.get('codigo_omie'):
                    # Caso onde o código da Omie não bate com ninguém do banco,
                    # mas é diferente do código do vendedor da planilha.
                    cod_esperado = str(int(dados_origem['codigo_omie'])).strip()
                    if cod_omie_real != cod_esperado:
                        status_auditoria = "DIVERGENCIA_VENDEDOR" # Fica com o original, mas marcado

            # ==================================================================

            # Gera ID Único baseado no VENDEDOR FINAL (O Dono Verdadeiro)
            # Isso impede duplicação: Se o Leonardo mandar o arquivo dele depois, 
            # vai bater o mesmo ID e apenas atualizar, sem criar venda dobrada.
            id_unico = f"{vendedor_final_nome}_{ano_ref}_{mes_ref:02d}_{pv_raw}".replace(" ", "_")

            hash_fields = [
                data_dt.strftime('%Y-%m-%d'), pv_raw, cliente, valor_ped, valor_pend, valor_comi
            ]
            
            return {
                "vendedor_nome_origem": vendedor_final_nome, # Vai para o banco como se fosse dele
                "vendedor_id": vendedor_final_id,            # UUID do dono verdadeiro
                "mes_referencia": mes_ref,
                "ano_referencia": ano_ref,
                "data_venda": data_dt.strftime('%Y-%m-%d'),
                "pv": pv_raw,
                "cliente": cliente,
                "valor_pedido": valor_ped,
                "valor_pendente": valor_pend,
                "valor_comissao": valor_comi,
                "id_unico_linha": id_unico,
                "status": "ATIVO",
                "tipo_registro": tipo_registro,
                
                # Auditoria
                "auditoria_status": status_auditoria,
                "vendedor_oficial": cod_omie_real,
                
                "_hash": DataSanitizer.generate_hash(hash_fields)
            }
        except Exception as e:
            logger.warning(f"⚠️ [LINHA {num_linha}] Erro: {str(e)}")
            return None

    def _get_month_number(self, mes_nome: str) -> Optional[int]:
        meses = {
            'JANEIRO': 1, 'FEVEREIRO': 2, 'MARÇO': 3, 'ABRIL': 4, 'MAIO': 5, 'JUNHO': 6,
            'JULHO': 7, 'AGOSTO': 8, 'SETEMBRO': 9, 'OUTUBRO': 10, 'NOVEMBRO': 11, 'DEZEMBRO': 12
        }
        return meses.get(mes_nome.upper())