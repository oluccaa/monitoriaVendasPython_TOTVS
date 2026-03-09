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

    def upsert_vendedores(self, pedidos: List[Dict[str, Any]]) -> bool:
        """
        Analisa o payload da TOTVS e cadastra/atualiza os vendedores no banco.
        Usa 'nome_planilha' como chave para não duplicar dados antigos.
        """
        if not pedidos: 
            return True

        vendedores_unicos = {}
        
        for p in pedidos:
            seller_id_str = str(p.get("sellerid", "")).strip()
            seller_name = str(p.get("sellername", "")).strip()
            
            # Se não vier nome do vendedor da TOTVS, ignora
            if not seller_name:
                continue

            # Prepara a conversão do código numérico
            seller_id_num = None
            if seller_id_str:
                try:
                    seller_id_num = float(seller_id_str)
                except ValueError:
                    pass

            # Agrupa usando o nome_planilha (nome bruto da TOTVS) para não processar o mesmo cara 50 vezes no loop
            if seller_name not in vendedores_unicos:
                vendedores_unicos[seller_name] = {
                    "nome_planilha": seller_name,
                    "nome_exibicao": seller_name.title(), # Ex: GABRIEL BARRETO -> Gabriel Barreto
                    "ativo": True
                }
                
                # Se a TOTVS enviou um ID numérico válido, adicionamos ao update
                if seller_id_num is not None:
                    vendedores_unicos[seller_name]["codigo_vendedor"] = seller_id_num

        lista_upsert = list(vendedores_unicos.values())
        
        if not lista_upsert:
            return True

        try:
            # O pulo do gato: on_conflict="nome_planilha" garante que os antigos sejam atualizados com o código novo
            self.client.table("vendedores").upsert(
                lista_upsert, 
                on_conflict="nome_planilha"
            ).execute()
            
            return True
        except Exception as e:
            logger.error(f"[SUPABASE] Erro ao sincronizar tabela de vendedores: {e}")
            return False 

    def upsert_pedidos(self, pedidos: List[Dict[str, Any]]) -> bool:
        """
        Envia pedidos mapeando exatamente para as colunas da tabela no Supabase.
        FILTRA DUPLICATAS no lote para evitar erro 21000.
        Utiliza 'id_unico_linha' como chave de conflito.
        """
        if not pedidos: return True

        pedidos_formatados = []
        ids_vistos = set() # Filtro de Duplicatas no Lote

        for p in pedidos:
            # Obtém o orderid bruto da TOTVS
            order_id = p.get("orderid")
            if not order_id:
                continue

            # Constrói o ID Único esperado pela constraint do banco de dados
            id_unico = p.get("id_unico_linha") or f"TOTVS-{order_id}"
            
            # Se ja vimos esse ID neste mesmo lote, ignoramos a copia
            if id_unico in ids_vistos:
                continue
            
            ids_vistos.add(id_unico)

            # Montagem do Objeto com chaves IDÊNTICAS às colunas do PostgreSQL
            registro = {
                "importacao_id": p.get("importacao_id"), 
                "pv": order_id, 
                "data_venda": p.get("issuedate"),
                "cliente": p.get("customername"),
                "vendedor_nome_origem": p.get("sellername"),
                "vendedor_oficial": p.get("sellername"), # Espelhando o vendedor para a coluna auxiliar
                "valor_pedido": float(p.get("amount", 0.0)),
                "valor_pendente": float(p.get("amount", 0.0)), # Como é novo, o pendente é igual ao pedido inicialmente
                "valor_comissao": 0.0, # Pode ser atualizado depois com o endpoint de comissões
                "mes_referencia": str(p.get("mes_referencia", "")),
                "ano_referencia": int(p.get("ano_referencia", 2026)),
                "status": p.get("status", "ATIVO"),
                "tipo_registro": p.get("tipo_registro", "CORRENTE"),
                "id_unico_linha": id_unico
            }
            
            pedidos_formatados.append(registro)

        try:
            # Upsert baseado na constraint unique "id_unico_linha" definida na tabela "vendas"
            self.client.table(self.table_vendas).upsert(
                pedidos_formatados, 
                on_conflict="id_unico_linha"
            ).execute()
            
            time.sleep(0.5) # Respeito ao Rate Limit da API do Supabase
            return True

        except Exception as e:
            logger.error(f"[SUPABASE] Erro no upsert: {e}")
            return False

# src/infrastructure/supabase_client.py

# import json
# import time
# import uuid
# from typing import List, Dict, Any, Optional
# from src.config import CONFIG
# from src.infrastructure.logging import logger

# class SupabaseRepository:
#     def __init__(self):
#         """
#         [MODO DE TESTE ATIVADO]
#         Cliente simulado. Nenhuma conexao real com o Supabase sera feita.
#         Os dados serao salvos em arquivos JSON locais na pasta 'data/json_tests'.
#         """
#         logger.info("[MOCK] Inicializando SupabaseRepository em MODO DE TESTE (JSON Local).")
        
#         # Cria a pasta para salvar os testes se ela nao existir
#         self.output_dir = CONFIG.BASE_DIR / "data" / "json_tests"
#         self.output_dir.mkdir(parents=True, exist_ok=True)

#     def criar_log_importacao(self, vendedor: str, mes: str, arquivo: str, qtd: int) -> Optional[str]:
#         """Simula a criacao do log no banco retornando um UUID falso."""
#         dummy_id = str(uuid.uuid4())
#         logger.info(f"[MOCK] Criando log falso de importacao para {vendedor}. ID gerado: {dummy_id}")
#         return dummy_id

#     def finalizar_log_importacao(self, log_id: str, status: str, erro: str = None):
#         """Simula a finalizacao do log."""
#         logger.info(f"[MOCK] Finalizando log falso {log_id} com status: {status}")

#     def upsert_pedidos(self, pedidos: List[Dict[str, Any]]) -> bool:
#         """
#         Em vez de enviar para a nuvem, formata os dados e salva em um arquivo JSON.
#         """
#         if not pedidos:
#             return True

#         pedidos_formatados = []
#         ids_vistos = set()

#         # O laco simula a formatacao exata que iria para o banco real
#         for p in pedidos:
#             id_unico = p.get("orderid")
            
#             if id_unico in ids_vistos:
#                 continue
#             ids_vistos.add(id_unico)

#             registro = {
#                 "importacao_id": p.get("importacao_id"),
#                 "orderid": id_unico,
#                 "issuedate": p.get("issuedate"),
#                 "sellerid": p.get("sellerid"),
#                 "amount": float(p.get("amount", 0.0)),
#                 "sellername": p.get("sellername"),
#                 "customername": p.get("customername"),
#                 "mes_referencia": str(p.get("mes_referencia", "")),
#                 "ano_referencia": p.get("ano_referencia", 2026),
#                 "status": p.get("status", "ATIVO"),
#                 "tipo_registro": p.get("tipo_registro", "NOVO"),
#                 "hash_controle": p.get("_hash", "")
#             }
#             pedidos_formatados.append(registro)

#         # Gera um nome de arquivo unico baseado no timestamp atual
#         timestamp_atual = time.strftime("%Y%m%d_%H%M%S")
#         nome_arquivo = f"mock_payload_{timestamp_atual}.json"
#         caminho_arquivo = self.output_dir / nome_arquivo

#         try:
#             # Salva o arquivo JSON com indentacao para facilitar o estudo e leitura
#             with open(caminho_arquivo, "w", encoding="utf-8") as f:
#                 json.dump(pedidos_formatados, f, ensure_ascii=False, indent=4)
                
#             logger.info(f"[MOCK] Sucesso. {len(pedidos_formatados)} pedidos salvos localmente em: {caminho_arquivo}")
            
#             # Simula a latencia de rede de uma chamada real a API
#             time.sleep(0.5) 
#             return True

#         except Exception as e:
#             logger.error(f"[MOCK] Erro ao salvar arquivo JSON local: {e}")
#             return False