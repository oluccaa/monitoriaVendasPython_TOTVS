# src/application/sync.py

import time
import hashlib
import asyncio
from typing import List, Dict, Any, TYPE_CHECKING
from src.infrastructure.logging import logger

if TYPE_CHECKING:
    from src.infrastructure.database import DatabaseRepository
    from src.infrastructure.supabase_client import SupabaseRepository

class SyncService:
    def __init__(self, repo: 'DatabaseRepository', client: 'SupabaseRepository'):
        """
        Servico de sincronizacao assincrona responsavel por calcular diferencas entre
        o TOTVS e o banco de dados final via cache local em paralelo.
        """
        self.repo = repo
        self.client = client
        self.max_batch_size = 200

    def _gerar_hash_pedido(self, pedido: Dict[str, Any]) -> str:
        """Gera um hash MD5 baseado nos valores criticos para detectar alteracoes."""
        orderid = str(pedido.get('orderid', '')).strip()
        issuedate = str(pedido.get('issuedate', '')).strip()
        amount = str(pedido.get('amount', 0.0)).strip()
        sellerid = str(pedido.get('sellerid', '')).strip()
        customername = str(pedido.get('customername', '')).strip()
        
        raw_string = f"{orderid}_{issuedate}_{amount}_{sellerid}_{customername}"
        return hashlib.md5(raw_string.encode('utf-8')).hexdigest()

    async def process_totvs_payload(self, pedidos_totvs: List[Dict[str, Any]]):
        """
        Orquestra o processamento do payload bruto do TOTVS de forma ASSINCRONA,
        protegendo o limite de sockets do Windows com um Semaforo.
        """
        start_time = time.perf_counter()
        logger.info("[SYNC] Iniciando processamento concorrente de payload TOTVS.")

        if not pedidos_totvs:
            logger.warning("[SYNC] Payload vazio recebido. Operacao abortada.")
            return

        # =================================================================
        # 1. LÓGICA DE BLACKLIST (ignorar_pedidos.json)
        # =================================================================
        import json
        import os
        
        pedidos_ignorados = set()
        caminho_blacklist = "ignorar_pedidos.json"
        
        if os.path.exists(caminho_blacklist):
            try:
                with open(caminho_blacklist, "r", encoding="utf-8") as f:
                    pedidos_ignorados = set(json.load(f))
            except Exception as e:
                logger.error(f"[SYNC] Falha ao ler a blacklist {caminho_blacklist}: {e}")

        # Filtra os pedidos brutos
        pedidos_filtrados = []
        for ped in pedidos_totvs:
            orderid = str(ped.get("orderid", "")).strip()
            if orderid in pedidos_ignorados:
                logger.info(f"[SYNC] Pedido {orderid} ignorado (Consta na Blacklist).")
                continue
            pedidos_filtrados.append(ped)

        if not pedidos_filtrados:
            logger.warning("[SYNC] Todos os pedidos do lote foram barrados pela Blacklist.")
            return

        # Substitui a lista original pela filtrada para seguir o fluxo normal
        pedidos_totvs = pedidos_filtrados
        # =================================================================

        # Executa a sincronizacao de vendedores em uma thread separada para nao travar o loop
        await asyncio.to_thread(self.client.upsert_vendedores, pedidos_totvs)

        grupos = {}
        for ped in pedidos_totvs:
            vendedor = ped.get("sellername", "DESCONHECIDO")
            data_emissao = ped.get("issuedate", "2026-01-01")
            
            try:
                partes_data = data_emissao.split("-")
                ano_ref = int(partes_data[0])
                mes_ref = str(int(partes_data[1]))
            except (IndexError, ValueError):
                ano_ref = 2026
                mes_ref = "01"
            
            ped["mes_referencia"] = mes_ref
            ped["ano_referencia"] = ano_ref

            chave = (vendedor, mes_ref, ano_ref)
            if chave not in grupos:
                grupos[chave] = []
            
            ped["_hash"] = self._gerar_hash_pedido(ped)
            ped["id_unico_linha"] = ped.get("orderid")
            grupos[chave].append(ped)

        # ---> O SEGREDO ESTA AQUI: SEMAFORO DE CONCORRENCIA <---
        # Limita a 5 grupos processados simultaneamente. Protege contra WinError 10035 e Rate Limits.
        sem = asyncio.Semaphore(3)

        async def process_with_semaphore(vendedor, mes_ref, ano_ref, pedidos_grupo):
            # O "async with sem:" garante que no maximo 5 execucoes passem desta linha ao mesmo tempo
            async with sem:
                try:
                    # Se _process_sync_group for uma função síncrona (def normal),
                    # use: await asyncio.to_thread(self._process_sync_group, vendedor, mes_ref, ano_ref, pedidos_grupo)
                    # Se já for async (async def), mantenha como está abaixo, 
                    # mas lembre-se de usar to_thread nas chamadas do Supabase lá dentro!
                    await self._process_sync_group(vendedor, mes_ref, ano_ref, pedidos_grupo)
                except Exception as e:
                    logger.error(f"[SYNC] Erro critico na thread de {vendedor} ({mes_ref}/{ano_ref}): {e}")

        # CRIAÇÃO DAS TASKS PARALELAS COM LIMITE
        # O uso do create_task é a forma mais segura e performática de empilhar rotinas no asyncio
        tasks = [
            asyncio.create_task(process_with_semaphore(vendedor, mes_ref, ano_ref, pedidos_grupo))
            for (vendedor, mes_ref, ano_ref), pedidos_grupo in grupos.items()
        ]

        # Executa as tarefas respeitando o pedágio do semáforo
        if tasks:
            # return_exceptions=True é VITAL no Windows:
            # Se a conexão de 1 vendedor cair, as outras 4 do semáforo continuam rodando normalmente.
            await asyncio.gather(*tasks, return_exceptions=True)

        duracao = time.perf_counter() - start_time
        logger.info(f"[SYNC] Sincronizacao do lote completo finalizada em {duracao:.2f}s")

    async def _process_sync_group(self, vendedor: str, mes_ref: str, ano_ref: int, pedidos_atuais: List[Dict[str, Any]]):
        """Calcula o Delta e realiza o envio para o Supabase de forma assincrona."""
        start_time = time.perf_counter()
        
        try:
            # 1. Protecao para o SQLite Local
            cache = await asyncio.to_thread(self.repo.get_cache_by_periodo, vendedor, mes_ref, ano_ref)
            pedidos_payload = []
            stats = {"new": 0, "upd": 0}

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

            if not pedidos_payload:
                return

            logger.info(f"[DELTA] {vendedor} ({mes_ref}/{ano_ref}): {stats['new']} novos, {stats['upd']} atualizados.")

            # Cria o Log no Supabase
            log_id = await asyncio.to_thread(
                self.client.criar_log_importacao,
                vendedor, mes_ref, f"TOTVS_API_{vendedor}_{mes_ref}_{ano_ref}", len(pedidos_payload)
            )

            # 2. Protecao contra Log_ID Nulo (Banco fora do ar)
            if not log_id:
                logger.error(f"[FAIL] {vendedor} | Nao foi possivel criar o Log_ID. Abortando envio deste grupo.")
                return

            # Injeta o ID da importacao nos pedidos
            for p in pedidos_payload:
                p["importacao_id"] = log_id

            chunks = [pedidos_payload[i:i + self.max_batch_size] for i in range(0, len(pedidos_payload), self.max_batch_size)]
            sucesso_total = True

            for chunk in chunks:
                upsert_ok = await asyncio.to_thread(self.client.upsert_pedidos, chunk)
                
                if upsert_ok:
                    await self._persist_changes(chunk, vendedor, mes_ref, ano_ref)
                else:
                    sucesso_total = False
                    logger.error(f"[FAIL] {vendedor} | Falha no envio do lote para {mes_ref}/{ano_ref}.")
                    break 

            status_final = "SUCESSO" if sucesso_total else "ERRO_PARCIAL"
            
            # Finaliza o log no Supabase
            await asyncio.to_thread(self.client.finalizar_log_importacao, log_id, status_final)

            duracao = time.perf_counter() - start_time
            if sucesso_total:
                logger.info(f"[DONE] {vendedor} | Periodo {mes_ref}/{ano_ref} processado em {duracao:.2f}s")

        except Exception as e:
            logger.error(f"[FATAL] Erro no grupo {vendedor} ({mes_ref}/{ano_ref}): {str(e)}", exc_info=True)
            
    async def _process_sync_group(self, vendedor: str, mes_ref: str, ano_ref: int, pedidos_atuais: List[Dict[str, Any]]):
        """Calcula o Delta e realiza o envio para o Supabase de forma assincrona."""
        start_time = time.perf_counter()
        
        try:
            # 1. Protecao para o SQLite Local
            cache = await asyncio.to_thread(self.repo.get_cache_by_periodo, vendedor, mes_ref, ano_ref)
            pedidos_payload = []
            stats = {"new": 0, "upd": 0}

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

            if not pedidos_payload:
                return

            logger.info(f"[DELTA] {vendedor} ({mes_ref}/{ano_ref}): {stats['new']} novos, {stats['upd']} atualizados.")

            # Cria o Log no Supabase
            log_id = await asyncio.to_thread(
                self.client.criar_log_importacao,
                vendedor, mes_ref, f"TOTVS_API_{vendedor}_{mes_ref}_{ano_ref}", len(pedidos_payload)
            )

            # 2. Protecao contra Log_ID Nulo (Banco fora do ar)
            if not log_id:
                logger.error(f"[FAIL] {vendedor} | Nao foi possivel criar o Log_ID. Abortando envio deste grupo.")
                return

            # Injeta o ID da importacao nos pedidos
            for p in pedidos_payload:
                p["importacao_id"] = log_id

            chunks = [pedidos_payload[i:i + self.max_batch_size] for i in range(0, len(pedidos_payload), self.max_batch_size)]
            sucesso_total = True

            for chunk in chunks:
                upsert_ok = await asyncio.to_thread(self.client.upsert_pedidos, chunk)
                
                if upsert_ok:
                    await self._persist_changes(chunk, vendedor, mes_ref, ano_ref)
                else:
                    sucesso_total = False
                    logger.error(f"[FAIL] {vendedor} | Falha no envio do lote para {mes_ref}/{ano_ref}.")
                    break 

            status_final = "SUCESSO" if sucesso_total else "ERRO_PARCIAL"
            
            # Finaliza o log no Supabase
            await asyncio.to_thread(self.client.finalizar_log_importacao, log_id, status_final)

            duracao = time.perf_counter() - start_time
            if sucesso_total:
                logger.info(f"[DONE] {vendedor} | Periodo {mes_ref}/{ano_ref} processado em {duracao:.2f}s")

        except Exception as e:
            logger.error(f"[FATAL] Erro no grupo {vendedor} ({mes_ref}/{ano_ref}): {str(e)}", exc_info=True)

    async def _persist_changes(self, vendas_chunk: List[Dict[str, Any]], vendedor: str, mes: str, ano: int):
        """Atualiza o cache local SQLite de forma transacional."""
        to_upsert = [] 
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

        for v in vendas_chunk:
            to_upsert.append((
                v["id_unico_linha"], 
                v.get("_hash", ""), 
                v.get("sellername", vendedor), 
                mes, 
                ano, 
                timestamp
            ))
        
        if to_upsert:
            try:
                await asyncio.to_thread(self.repo.update_batch, [], to_upsert)
            except Exception as e:
                logger.error(f"[CACHE] Falha ao persistir alteracoes locais: {e}")