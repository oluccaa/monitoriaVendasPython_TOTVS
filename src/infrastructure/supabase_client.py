# src/infrastructure/supabase_client.py

import time
from typing import List, Dict, Any, Optional
from supabase import create_client, Client
from src.config import CONFIG
from src.infrastructure.logging import logger

class SupabaseRepository:
    def __init__(self):
        """
        Cliente Supabase configurado para o Schema SQL de Pedidos TOTVS.
        """
        if not CONFIG.SUPABASE_URL or not CONFIG.SUPABASE_KEY:
            raise ValueError("[CONFIG] Faltam SUPABASE_URL e SUPABASE_KEY no .env")

        try:
            self.client: Client = create_client(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_KEY)
            self.table_vendas = "vendas" # Ou "pedidos", ajuste conforme o nome da sua tabela no Supabase
            self.table_logs = "logs_importacao"
        except Exception as e:
            logger.critical(f"[SUPABASE] Falha ao inicializar cliente: {e}")
            raise

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
            res = self.client.table(self.table_logs).insert(data).execute()
            
            if res.data and len(res.data) > 0:
                return res.data[0]['id']
            return None
        except Exception as e:
            logger.error(f"[SUPABASE] Erro ao criar log de importacao: {e}")
            return None

    def finalizar_log_importacao(self, log_id: str, status: str, erro: str = None):
        """Atualiza o status do log de importação ao final do processo."""
        if not log_id: return
        try:
            update_data = {"status": status}
            if erro:
                update_data["mensagem_erro"] = erro[:500] 

            self.client.table(self.table_logs).update(update_data).eq("id", log_id).execute()
        except Exception as e:
            logger.error(f"[SUPABASE] Erro ao finalizar log {log_id}: {e}")

    def upsert_pedidos(self, pedidos: List[Dict[str, Any]]) -> bool:
        """
        Envia pedidos mapeando exatamente para as colunas da tabela no Supabase.
        FILTRA DUPLICATAS no lote para evitar erro 21000.
        Utiliza 'orderid' como chave de conflito.
        """
        if not pedidos: return True

        pedidos_formatados = []
        ids_vistos = set() # Filtro de Duplicatas no Lote

        for p in pedidos:
            # No TOTVS, o identificador único e absoluto é o orderid
            id_unico = p.get("orderid")
            
            # Se ja vimos esse ID neste mesmo lote, ignoramos a copia
            if id_unico in ids_vistos:
                continue
            
            ids_vistos.add(id_unico)

            # Montagem do Objeto (Payload ajustado para o TOTVS)
            registro = {
                "importacao_id": p.get("importacao_id"), # UUID FK
                "orderid": id_unico,                     # Chave do Upsert
                "issuedate": p.get("issuedate"),
                "sellerid": p.get("sellerid"),
                "amount": float(p.get("amount", 0.0)),
                "sellername": p.get("sellername"),
                "customername": p.get("customername"),
                
                # Dados de particionamento e controle criados no sync.py
                "mes_referencia": str(p.get("mes_referencia", "")),
                "ano_referencia": p.get("ano_referencia", 2026),
                "status": p.get("status", "ATIVO"),
                "tipo_registro": p.get("tipo_registro", "NOVO"),
                "hash_controle": p.get("_hash", "")
            }
            pedidos_formatados.append(registro)

        try:
            # Upsert baseado na constraint unique/primary key (orderid)
            # Certifique-se de que a coluna "orderid" seja PRIMARY KEY no PostgresSQL do Supabase
            self.client.table(self.table_vendas).upsert(pedidos_formatados, on_conflict="orderid").execute()
            
            time.sleep(0.5) # Respeito ao Rate Limit da API do Supabase
            return True

        except Exception as e:
            logger.error(f"[SUPABASE] Erro no upsert: {e}")
            return False