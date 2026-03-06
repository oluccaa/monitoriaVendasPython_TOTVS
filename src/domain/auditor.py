# src/domain/auditor.py

import json
import os
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from src.config import CONFIG
from src.infrastructure.logging import logger
from src.domain.sanitizer import DataSanitizer

# ==============================================================================
# SERVIÇO DE DOMÍNIO: AUDITORIA HÍBRIDA (CACHE JSON INTELIGENTE)
# ==============================================================================

class OmieCacheManager:
    """
    Gerencia o download e armazenamento local dos dados da Omie.
    Transforma a API lenta em um JSON local ultra-rápido.
    """
    def __init__(self):
        self.cache_dir = "data/omie_cache"
        self.endpoint = "https://app.omie.com.br/api/v1/produtos/pedido/"
        self.headers = {'Content-Type': 'application/json'}
        
        # Cria a pasta de cache se não existir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, mes: int, ano: int) -> str:
        return os.path.join(self.cache_dir, f"omie_{ano}_{mes:02d}.json")

    def precisa_atualizar(self, mes: int, ano: int, force: bool = False) -> bool:
        """
        Verifica se deve baixar dados novos.
        Retorna True se:
        1. A flag 'force' for True (Modo Sentinela/Atualização Forçada).
        2. O arquivo não existir.
        3. O arquivo for mais velho que 1 hora (3600s).
        """
        if force:
            return True

        path = self._get_cache_path(mes, ano)
        if not os.path.exists(path):
            return True
        
        # Se o arquivo for mais velho que 60 minutos (3600s), atualiza
        file_age = time.time() - os.path.getmtime(path)
        return file_age > 3600

    def baixar_dados_omie(self, mes: int, ano: int) -> Dict[str, dict]:
        """
        Baixa TODOS os pedidos do mês via paginação (ListarPedidos).
        """
        logger.info(f"⬇️ [OMIE DOWNLOAD] Baixando massa de dados para {mes:02d}/{ano}...")
        
        data_inicio = f"01/{mes:02d}/{ano}"
        prox_mes = mes + 1 if mes < 12 else 1
        prox_ano = ano if mes < 12 else ano + 1
        ultimo_dia = (datetime(prox_ano, prox_mes, 1) - timedelta(days=1)).strftime("%d/%m/%Y")
        data_fim = ultimo_dia

        pedidos_temp = {}
        pagina = 1
        total_paginas = 1

        while pagina <= total_paginas:
            payload = {
                "call": "ListarPedidos",
                "app_key": CONFIG.OMIE_APP_KEY,
                "app_secret": CONFIG.OMIE_APP_SECRET,
                "param": [{
                    "pagina": pagina,
                    "registros_por_pagina": 100, 
                    "filtrar_por_data_de": data_inicio,
                    "filtrar_por_data_ate": data_fim,
                    "filtrar_apenas_inclusao": "S",
                    "apenas_resumo": "N"
                }]
            }

            try:
                r = requests.post(self.endpoint, json=payload, headers=self.headers, timeout=20)
                r.raise_for_status()
                data = r.json()

                total_paginas = data.get("total_de_paginas", 1)
                lista = data.get("pedido_venda_produto", [])

                for p in lista:
                    cabecalho = p.get("cabecalho", {})
                    total = p.get("total_pedido", {})
                    info = p.get("info_cadastro", {})
                    
                    # 1. Número do Pedido
                    pv = str(cabecalho.get("numero_pedido", "")).strip()
                    if not pv: 
                        continue
                    
                    # 2. Código do Vendedor
                    # Fica dentro de informacoes_adicionais -> codVend
                    info_adic = p.get("informacoes_adicionais", {})
                    cod_vendedor = str(info_adic.get("codVend", "")).strip()

                    # 3. Status Cancelado (Essa lógica garante a atualização do status)
                    cancelado = (
                        info.get("cancelado") == "S" or 
                        cabecalho.get("cancelado") == "S" or
                        p.get("status_pedido") == "CANCELADO"
                    )

                    # 4. Valor
                    valor = float(total.get("valor_total_pedido", 0.0))

                    # Sobrescreve/Adiciona ao dicionário temporário
                    pedidos_temp[pv] = {
                        "valor": valor,
                        "cancelado": cancelado,
                        "codigo_vendedor": cod_vendedor
                    }

                logger.debug(f"   📄 Página {pagina}/{total_paginas} ok ({len(lista)} itens)")
                pagina += 1

            except Exception as e:
                logger.error(f"💥 [OMIE DOWNLOAD] Erro na página {pagina}: {e}")
                return {}

        # Salva no disco (Sobrescreve o arquivo antigo completamente com 'w')
        path = self._get_cache_path(mes, ano)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(pedidos_temp, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ [OMIE DOWNLOAD] Cache salvo/atualizado: {path} ({len(pedidos_temp)} pedidos)")
        except Exception as e:
            logger.error(f"💥 [CACHE SAVE] Erro ao salvar JSON: {e}")

        return pedidos_temp

    def carregar_cache(self, mes: int, ano: int) -> Dict[str, dict]:
        """Lê o JSON do disco para a memória."""
        path = self._get_cache_path(mes, ano)
        if not os.path.exists(path):
            return {}
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"⚠️ [CACHE] JSON corrompido: {e}")
            return {}


class AuditService:
    def __init__(self):
        self.manager = OmieCacheManager()
        self.memoria_atual = {}
        self.contexto_atual = ""

    def prepare_context(self, mes: int, ano: int, force_update: bool = False):
        """
        Garante que temos os dados desse mês carregados na RAM.
        
        Args:
            mes (int): Mês alvo.
            ano (int): Ano alvo.
            force_update (bool): Se True, força o download da Omie mesmo que o cache seja recente.
        """
        chave = f"{mes}-{ano}"
        
        # Verifica se precisa atualizar (tempo expirado ou force_update ativado)
        precisa = self.manager.precisa_atualizar(mes, ano, force=force_update)

        # Se o contexto mudou OU precisamos atualizar
        if self.contexto_atual != chave or precisa:
            
            if precisa:
                logger.info(f"🔄 [AUDITOR] Atualização necessária (Force={force_update}). Baixando...")
                dados_novos = self.manager.baixar_dados_omie(mes, ano)
                
                if dados_novos:
                    self.memoria_atual = dados_novos
                else:
                    # Se falhar o download, tenta carregar o cache antigo para não quebrar
                    logger.warning("⚠️ [AUDITOR] Falha no download, usando cache existente.")
                    self.memoria_atual = self.manager.carregar_cache(mes, ano)
            else:
                # Apenas carrega do disco se não precisar baixar
                self.memoria_atual = self.manager.carregar_cache(mes, ano)
            
            self.contexto_atual = chave
            logger.info(f"🧠 [AUDITOR] Contexto carregado: {len(self.memoria_atual)} PVs na memória.")

    def auditar_linha(self, pv: str, valor_excel: float = 0.0) -> Dict[str, Any]:
        """
        Consulta O(1) na memória RAM.
        """
        pv_limpo = str(pv).strip()

        # 1. Verifica Existência
        dados_omie = self.memoria_atual.get(pv_limpo)

        if not dados_omie:
            return {
                "manifesto": "S",
                "status_auditoria": "NAO_ENCONTRADO_OMIE",
                "divergencia": "PV não encontrado (Cache Omie)",
                "codigo_vendedor": ""
            }

        # 2. Verifica Valor
        valor_omie = dados_omie["valor"]
        diferenca = abs(valor_excel - valor_omie)
        
        status = "APROVADO"
        msg_div = ""

        if diferenca > 0.10:
            status = "DIVERGENTE"
            msg_div = f"Omie: {valor_omie:.2f} | Excel: {valor_excel:.2f}"

        # 3. Verifica Cancelamento
        # Se na Omie agora constar como cancelado (mesmo que antes não fosse), o cache atualizado vai pegar
        if dados_omie["cancelado"]:
            status = "REPROVADO"
            msg_div = "Pedido CANCELADO na Omie"

        return {
            "manifesto": "S" if status != "APROVADO" else "N",
            "status_auditoria": status,
            "divergencia": msg_div,
            "codigo_vendedor": dados_omie.get("codigo_vendedor", "")
        }

# Instância Global
AUDITOR = AuditService()