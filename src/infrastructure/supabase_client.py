# src/infrastructure/supabase_client.py

import time
from typing import List, Dict, Any, Optional
from supabase import create_client, Client
from src.config import CONFIG
from src.infrastructure.logging import logger

class SupabaseRepository:
    def __init__(self):
        """
        Cliente Supabase configurado para o Schema SQL fornecido.
        """
        if not CONFIG.SUPABASE_URL or not CONFIG.SUPABASE_KEY:
            raise ValueError("❌ [CONFIG] Faltam SUPABASE_URL e SUPABASE_KEY no .env")

        try:
            self.client: Client = create_client(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_KEY)
            self.table_vendas = "vendas"
            self.table_logs = "logs_importacao"
        except Exception as e:
            logger.critical(f"💥 [SUPABASE] Falha ao inicializar cliente: {e}")
            raise

    def get_vendedores_ativos(self) -> dict:
        """
        Retorna dicionário rico: 
        { 
            'NOME_PLANILHA': {
                'id': 'uuid-123...', 
                'codigo_omie': 12345 (ou None)
            } 
        }
        Filtrando apenas por vendedores ATIVOS.
        """
        try:
            # Busca ID, Nome e Código Omie apenas de quem está ATIVO
            res = self.client.table("vendedores")\
                .select("id, nome_planilha, codigo_vendedor")\
                .eq("ativo", True)\
                .execute()
            
            # Cria mapa rico para uso no ExcelProcessor
            cache = {}
            for row in res.data:
                # Normalização básica para evitar erros de chave
                nome_chave = str(row['nome_planilha']).strip()
                
                cache[nome_chave] = {
                    'id': row['id'],
                    'codigo_omie': row.get('codigo_vendedor') # Pode ser None
                }
            
            return cache

        except Exception as e:
            logger.error(f"❌ [SUPABASE] Falha crítica ao carregar vendedores: {e}")
            return {}

    def criar_log_importacao(self, vendedor: str, mes: str, arquivo: str, qtd: int) -> Optional[str]:
        """
        Cria o log e retorna o UUID (string) gerado pelo banco.
        """
        try:
            data = {
                "vendedor_nome": vendedor,
                "mes_referencia": str(mes),
                "arquivo_nome": arquivo,
                "linhas_processadas": qtd,
                "status": "PROCESSANDO",
                "data_processamento": time.strftime('%Y-%m-%d %H:%M:%S')
            }
            # .execute() retorna um objeto com .data
            res = self.client.table(self.table_logs).insert(data).execute()
            
            if res.data and len(res.data) > 0:
                # O Supabase retorna o UUID criado automaticamente
                return res.data[0]['id']
            return None
        except Exception as e:
            logger.error(f"⚠️ [SUPABASE] Erro ao criar log de importação: {e}")
            return None

    def finalizar_log_importacao(self, log_id: str, status: str, erro: str = None):
        if not log_id: return
        try:
            update_data = {"status": status}
            if erro:
                update_data["mensagem_erro"] = erro[:500] 

            self.client.table(self.table_logs).update(update_data).eq("id", log_id).execute()
        except Exception as e:
            logger.error(f"⚠️ [SUPABASE] Erro ao finalizar log {log_id}: {e}")

    def upsert_lote_vendas(self, vendas: List[Dict[str, Any]]) -> bool:
        """
        Envia vendas mapeando exatamente para as colunas da tabela public.vendas.
        FILTRA DUPLICATAS para evitar erro 21000.
        """
        if not vendas: return True

        vendas_formatadas = []
        ids_vistos = set() # 🛡️ Filtro de Duplicatas no Lote

        for v in vendas:
            id_unico = v.get("id_unico_linha")
            
            # Se já vimos esse ID neste mesmo lote, ignoramos a cópia
            if id_unico in ids_vistos:
                continue
            
            ids_vistos.add(id_unico)

            # 1. Tratamento de Tipos para o SQL
            mes_ref = str(v.get("mes_referencia", "")) # SQL pede TEXT
            
            # 2. Montagem do Objeto (Payload)
            registro = {
                "importacao_id": v.get("importacao_id"), # UUID FK
                "vendedor_nome_origem": v.get("vendedor_nome_origem"),
                "mes_referencia": mes_ref,
                "ano_referencia": v.get("ano_referencia", 2026),
                "data_venda": v.get("data_venda"),
                "pv": v.get("pv"),
                "cliente": v.get("cliente"),
                "valor_pedido": v.get("valor_pedido", 0.0),
                "valor_pendente": v.get("valor_pendente", 0.0),
                "valor_comissao": v.get("valor_comissao", 0.0),
                "id_unico_linha": id_unico, # Chave do Upsert
                "status": v.get("status", "ATIVO"),
                "tipo_registro": v.get("tipo_registro", "CORRENTE"),
                
                # Campos de Auditoria (Novos)
                "auditoria_status": v.get("auditoria_status"),
                "vendedor_oficial": v.get("vendedor_oficial")
            }
            vendas_formatadas.append(registro)

        try:
            # Upsert baseado na constraint unique (id_unico_linha)
            self.client.table(self.table_vendas).upsert(vendas_formatadas, on_conflict="id_unico_linha").execute()
            
            time.sleep(0.5) # Respeito ao Rate Limit
            return True

        except Exception as e:
            logger.error(f"❌ [SUPABASE] Erro no upsert: {e}")
            return False