import time
import hashlib
from typing import List, Dict, Any, TYPE_CHECKING
from src.infrastructure.logging import logger

if TYPE_CHECKING:
    from src.infrastructure.database import DatabaseRepository
    from src.infrastructure.supabase_client import SupabaseRepository

class SyncService:
    def __init__(self, repo: 'DatabaseRepository', client: 'SupabaseRepository'):
        self.repo = repo
        self.client = client
        self.max_batch_size = 200

    def _gerar_hash_pedido(self, pedido: Dict[str, Any]) -> str:
        orderid = str(pedido.get('orderid', '')).strip()
        issuedate = str(pedido.get('issuedate', '')).strip()
        amount = str(pedido.get('amount', 0.0)).strip()
        sellerid = str(pedido.get('sellerid', '')).strip()
        customername = str(pedido.get('customername', '')).strip()
        raw_string = f"{orderid}_{issuedate}_{amount}_{sellerid}_{customername}"
        return hashlib.md5(raw_string.encode('utf-8')).hexdigest()

    def process_totvs_payload(self, pedidos_totvs: List[Dict[str, Any]]):
        start_time = time.perf_counter()
        logger.info("[INIT] Iniciando processamento do payload TOTVS.")

        if not pedidos_totvs:
            logger.warning("[SKIP] Payload vazio. Nenhum dado para processar.")
            return

        grupos = {}
        for ped in pedidos_totvs:
            vendedor = ped.get("sellername", "DESCONHECIDO")
            data_emissao = ped.get("issuedate", "2026-01-01")
            
            try:
                ano_ref = int(data_emissao.split("-")[0])
                mes_ref = str(int(data_emissao.split("-")[1]))
            except (IndexError, ValueError):
                ano_ref = 2026
                mes_ref = "01"
            
            chave = (vendedor, mes_ref, ano_ref)
            if chave not in grupos:
                grupos[chave] = []
            
            ped["_hash"] = self._gerar_hash_pedido(ped)
            ped["id_unico_linha"] = ped.get("orderid")
            grupos[chave].append(ped)

        for (vendedor, mes_ref, ano_ref), pedidos_grupo in grupos.items():
            self._process_sync_group(vendedor, mes_ref, ano_ref, pedidos_grupo)

        duracao = time.perf_counter() - start_time
        logger.info(f"[DONE] Sincronizacao total do lote concluida em {duracao:.2f}s")

    def _process_sync_group(self, vendedor: str, mes_ref: str, ano_ref: int, pedidos_atuais: List[Dict[str, Any]]):
        start_time = time.perf_counter()
        
        try:
            cache = self.repo.get_cache_by_periodo(vendedor, mes_ref, ano_ref)
            pedidos_payload = []
            stats = {"new": 0, "upd": 0, "del": 0}

            for ped in pedidos_atuais:
                id_l = ped["id_unico_linha"]
                curr_hash = ped["_hash"]
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

            # Bloco de exclusao retirado por seguranca em arquitetura de polling
            # Evita apagar pedidos se o TOTVS enviar apenas uma lista parcial

            if not pedidos_payload:
                return

            logger.info(f"[DELTA] {vendedor} ({mes_ref}/{ano_ref}) -> +{stats['new']} Novos | ~{stats['upd']} Updates")

            log_id = self.client.criar_log_importacao(
                vendedor=vendedor, mes=mes_ref, arquivo=f"TOTVS_{vendedor}_{mes_ref}_{ano_ref}", qtd=len(pedidos_payload)
            )

            if log_id:
                for p in pedidos_payload:
                    p["importacao_id"] = log_id

            chunks = [pedidos_payload[i:i + self.max_batch_size] for i in range(0, len(pedidos_payload), self.max_batch_size)]
            
            total_chunks = len(chunks)
            sucesso_total = True

            for i, chunk in enumerate(chunks, 1):
                if self.client.upsert_pedidos(chunk):
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
        to_upsert = [] 
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

        for v in vendas_chunk:
            to_upsert.append((
                v["id_unico_linha"], v.get("_hash", ""), v.get("sellername", vendedor), mes, ano, timestamp
            ))
        
        if to_upsert:
            try:
                self.repo.update_batch([], to_upsert)
            except Exception as e:
                logger.error(f"[CACHE ERROR] Falha ao persistir alteracoes locais: {e}")