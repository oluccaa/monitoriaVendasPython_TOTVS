# src/application/sync.py

# 1. Imports da Biblioteca Padrão
import time
import hashlib
from typing import List, Dict, Any, TYPE_CHECKING

# 2. Imports da Infraestrutura
from src.infrastructure.logging import logger
from src.config import CONFIG

# Uso de TYPE_CHECKING para evitar erros de importação circular
if TYPE_CHECKING:
    from src.infrastructure.database import DatabaseRepository
    from src.infrastructure.supabase_client import SupabaseRepository

# ==============================================================================
# SERVIÇO: SINCRONIZAÇÃO TOTVS (PARTICIONAMENTO TEMPORAL E DELTA)
# ==============================================================================

class SyncService:
    def __init__(self, repo: 'DatabaseRepository', client: 'SupabaseRepository'):
        self.repo = repo
        self.client = client
        self.max_batch_size = 200

    def _gerar_hash_pedido(self, pedido: Dict[str, Any]) -> str:
        """Gera um hash MD5 baseado nos valores essenciais do pedido TOTVS para detectar atualizações."""
        raw_string = f"{pedido.get('orderid', '')}{pedido.get('issuedate', '')}{pedido.get('amount', '')}{pedido.get('sellerid', '')}{pedido.get('customername', '')}"
        return hashlib.md5(raw_string.encode('utf-8')).hexdigest()

    def process_totvs_payload(self, pedidos_totvs: List[Dict[str, Any]]):
        """
        Recebe o payload bruto do TOTVS, agrupa por vendedor e período,
        e orquestra a sincronização de Delta mantendo a segurança do cache local.
        """
        start_time = time.perf_counter()
        logger.info("[INIT] Iniciando processamento do payload TOTVS.")

        if not pedidos_totvs:
            logger.warning("[SKIP] Payload vazio. Nenhum dado para processar.")
            return

        # 1. Agrupamento Inteligente
        # Como o TOTVS envia um lote contendo múltiplos vendedores e datas,
        # agrupamos os dados para manter o isolamento de segurança do banco local.
        grupos = {}
        for ped in pedidos_totvs:
            vendedor = ped.get("sellername", "DESCONHECIDO")
            data_emissao = ped.get("issuedate", "2026-01-01")
            
            try:
                # Ex: "2026-02-02" -> Ano 2026, Mês "02"
                ano_ref = int(data_emissao.split("-")[0])
                mes_ref = str(int(data_emissao.split("-")[1]))
            except (IndexError, ValueError):
                ano_ref = 2026
                mes_ref = "01"
            
            chave = (vendedor, mes_ref, ano_ref)
            if chave not in grupos:
                grupos[chave] = []
            
            # Prepara chaves internas usadas pelo motor de Delta
            ped["_hash"] = self._gerar_hash_pedido(ped)
            ped["id_unico_linha"] = ped.get("orderid")
            
            grupos[chave].append(ped)

        # 2. Processamento por Grupo (Isolado)
        for (vendedor, mes_ref, ano_ref), pedidos_grupo in grupos.items():
            self._process_sync_group(vendedor, mes_ref, ano_ref, pedidos_grupo)

        duracao = time.perf_counter() - start_time
        logger.info(f"[DONE] Sincronizacao total do lote TOTVS concluida em {duracao:.2f}s")

    def _process_sync_group(self, vendedor: str, mes_ref: str, ano_ref: int, pedidos_atuais: List[Dict[str, Any]]):
        """Executa a lógica de Delta com segurança de período para um grupo específico."""
        start_time = time.perf_counter()
        
        try:
            logger.info(f"[INIT] {vendedor} | Analisando periodo {mes_ref}/{ano_ref}")
            
            # 1. CACHE PARTICIONADO
            cache = self.repo.get_cache_by_periodo(vendedor, mes_ref, ano_ref)
            
            pedidos_payload = []
            ids_na_api = set()
            stats = {"new": 0, "upd": 0, "del": 0}

            # 2. Delta Positivo (Inserções e Atualizações)
            for ped in pedidos_atuais:
                id_l = ped["id_unico_linha"]
                curr_hash = ped["_hash"]
                ids_na_api.add(id_l)

                cached_hash = cache.get(id_l)

                if not cached_hash:
                    ped["tipo_registro"] = "NOVO"
                    ped["status"] = "ATIVO"
                    pedidos_payload.append(ped)
                    stats["new"] += 1
                elif cached_hash != curr_hash:
                    ped["tipo_registro"] = "ATUALIZADO"
                    ped["status"] = "ATIVO"
                    pedidos_payload.append(ped)
                    stats["upd"] += 1

            # 3. Delta Negativo (Exclusões Lógicas de pedidos deletados no TOTVS)
            ids_deletados = set(cache.keys()) - ids_na_api
            
            for id_del in ids_deletados:
                pedidos_payload.append({
                    "orderid": id_del,
                    "id_unico_linha": id_del,
                    "sellername": vendedor,
                    "mes_referencia": mes_ref, 
                    "ano_referencia": ano_ref,
                    "status": "DELETADO",
                    "tipo_registro": "EXCLUSAO_LOGICA",
                    "_hash": "DELETADO"
                })
                stats["del"] += 1

            # 4. Verificação de Ociosidade
            if not pedidos_payload:
                logger.info(f"[IDLE] {vendedor} | {mes_ref}/{ano_ref} ja esta sincronizado.")
                return

            logger.info(f"[DELTA] {vendedor} ({mes_ref}/{ano_ref}) -> +{stats['new']} Novos | ~{stats['upd']} Updates | -{stats['del']} Removidos")

            # ==================================================================
            # 5. ORQUESTRAÇÃO SUPABASE
            # ==================================================================
            
            # Registro de auditoria da importação
            log_id = self.client.criar_log_importacao(
                vendedor=vendedor,
                mes=mes_ref,
                arquivo=f"TOTVS_{vendedor}_{mes_ref}_{ano_ref}",
                qtd=len(pedidos_payload)
            )

            if log_id:
                for p in pedidos_payload:
                    p["importacao_id"] = log_id

            # Envio em Lotes
            chunks = [pedidos_payload[i:i + self.max_batch_size] 
                      for i in range(0, len(pedidos_payload), self.max_batch_size)]
            
            total_chunks = len(chunks)
            sucesso_total = True

            for i, chunk in enumerate(chunks, 1):
                # Utiliza o método correspondente no cliente Supabase
                if self.client.upsert_pedidos(chunk):
                    # Grava no banco local APENAS se o Supabase confirmou sucesso
                    self._persist_changes(chunk, vendedor, mes_ref, ano_ref)
                else:
                    sucesso_total = False
                    logger.error(f"[FAIL] {vendedor} | Falha no lote {i}/{total_chunks}. Operacao interrompida.")
                    break 

            status_final = "SUCESSO" if sucesso_total else "ERRO_PARCIAL"
            self.client.finalizar_log_importacao(log_id, status_final)

            duracao = time.perf_counter() - start_time
            if sucesso_total:
                logger.info(f"[DONE] {vendedor} | {mes_ref}/{ano_ref} finalizado em {duracao:.2f}s")

        except Exception as e:
            logger.error(f"[FATAL] {vendedor} ({mes_ref}/{ano_ref}) | Erro critico: {str(e)}", exc_info=True)

    def _persist_changes(self, vendas_chunk: List[Dict[str, Any]], vendedor: str, mes: str, ano: int):
        """
        Mantém o cache do SQLite local atualizado de forma transacional.
        """
        to_delete = [] 
        to_upsert = [] 
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

        for v in vendas_chunk:
            if v.get("status") == "DELETADO":
                to_delete.append((v["id_unico_linha"],))
            else:
                to_upsert.append((
                    v["id_unico_linha"],
                    v.get("_hash", ""),
                    v.get("sellername", vendedor),
                    mes,
                    ano,
                    timestamp
                ))
        
        if to_delete or to_upsert:
            try:
                self.repo.update_batch(to_delete, to_upsert)
            except Exception as e:
                logger.error(f"[CACHE ERROR] Falha ao persistir alteracoes locais: {e}")