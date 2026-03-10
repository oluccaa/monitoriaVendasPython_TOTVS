# src/application/sync.py

import asyncio
import hashlib
import json
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from src.infrastructure.logging import logger

if TYPE_CHECKING:
    from src.infrastructure.database import DatabaseRepository
    from src.infrastructure.supabase_client import SupabaseRepository


class SyncService:
    """
    Serviço de sincronização assíncrona responsável por:

    1. Normalizar e validar pedidos vindos do TOTVS
    2. Aplicar blacklist (ignorar_pedidos.json)
    3. Agrupar pedidos por vendedor/mês/ano
    4. Calcular delta com base no cache local
    5. Enviar apenas NOVOS e ATUALIZADOS ao destino final
    6. Persistir hash localmente após sucesso

    Observação:
    - Esta versão mantém o contrato atual do sistema:
      * repo.get_cache_by_periodo(vendedor, mes, ano) -> dict[id_unico_linha, hash]
      * repo.update_batch(to_delete, to_upsert)
      * client.upsert_vendedores(pedidos)
      * client.criar_log_importacao(...)
      * client.upsert_pedidos(chunk)
      * client.finalizar_log_importacao(log_id, status)
    """

    def __init__(self, repo: "DatabaseRepository", client: "SupabaseRepository"):
        self.repo = repo
        self.client = client
        self.max_batch_size = 200
        self.max_concurrent_groups = 3

        # Cache simples da blacklist para evitar reler arquivo toda hora
        self._blacklist_cache: Set[str] = set()
        self._blacklist_mtime: Optional[float] = None

    # =========================================================
    # NORMALIZAÇÃO
    # =========================================================

    def _normalizar_orderid(self, raw_id: Any, largura: int = 6) -> str:
        """
        Normaliza orderid para comparação confiável.

        Exemplos:
        281         -> 000281
        "281"       -> 000281
        "281.0"     -> 000281
        "000281"    -> 000281
        " 281 "     -> 000281
        None        -> ""
        """
        if raw_id is None:
            return ""

        # int puro
        if isinstance(raw_id, int):
            return str(raw_id).zfill(largura)

        # float puro
        if isinstance(raw_id, float):
            if raw_id.is_integer():
                return str(int(raw_id)).zfill(largura)
            return ""

        valor = str(raw_id).strip()
        if not valor:
            return ""

        # Caso comum: "281.0"
        if re.fullmatch(r"\d+\.0+", valor):
            valor = valor.split(".")[0]

        # Extrai apenas dígitos
        valor = re.sub(r"\D", "", valor)

        if not valor:
            return ""

        return valor.zfill(largura)

    def _normalizar_texto(self, valor: Any) -> str:
        """
        Remove espaços excessivos e padroniza caixa.
        """
        return " ".join(str(valor or "").strip().split()).upper()

    def _normalizar_amount(self, raw_amount: Any) -> str:
        """
        Padroniza valor monetário com duas casas decimais.
        """
        if raw_amount is None:
            return "0.00"

        try:
            valor = Decimal(str(raw_amount).strip().replace(",", "."))
            return f"{valor:.2f}"
        except (InvalidOperation, ValueError, TypeError):
            return "0.00"

    def _normalizar_data(self, raw_date: Any) -> str:
        """
        Converte datas para YYYY-MM-DD.
        Retorna "" se não conseguir interpretar.
        """
        if raw_date is None:
            return ""

        valor = str(raw_date).strip()
        if not valor:
            return ""

        formatos = (
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y",
            "%d/%m/%Y %H:%M:%S",
            "%Y/%m/%d",
            "%Y/%m/%d %H:%M:%S",
        )

        for fmt in formatos:
            try:
                dt = datetime.strptime(valor, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return ""

    # =========================================================
    # HASH
    # =========================================================

    def _gerar_hash_pedido(self, pedido: Dict[str, Any]) -> str:
        """
        Gera hash com base em campos críticos normalizados.
        """
        orderid = self._normalizar_orderid(pedido.get("orderid"))
        issuedate = self._normalizar_data(pedido.get("issuedate"))
        amount = self._normalizar_amount(pedido.get("amount"))
        sellerid = self._normalizar_texto(pedido.get("sellerid"))
        customername = self._normalizar_texto(pedido.get("customername"))

        raw_string = f"{orderid}|{issuedate}|{amount}|{sellerid}|{customername}"
        return hashlib.md5(raw_string.encode("utf-8")).hexdigest()

    # =========================================================
    # BLACKLIST
    # =========================================================

    def _blacklist_path(self) -> Path:
        """
        Resolve o caminho do arquivo ignorar_pedidos.json.
        Considerando:
        src/application/sync.py -> raiz do projeto = parents[2]
        """
        return Path(__file__).resolve().parents[2] / "ignorar_pedidos.json"

    def _carregar_blacklist(self) -> Set[str]:
        """
        Carrega e normaliza a blacklist.
        Usa cache simples por mtime do arquivo.
        """
        caminho = self._blacklist_path()

        if not caminho.exists():
            logger.warning(f"[SYNC] ALERTA: Arquivo de blacklist não encontrado em: {caminho}")
            self._blacklist_cache = set()
            self._blacklist_mtime = None
            return self._blacklist_cache

        try:
            mtime_atual = caminho.stat().st_mtime

            # Reutiliza cache se o arquivo não mudou
            if self._blacklist_mtime == mtime_atual:
                return self._blacklist_cache

            with caminho.open("r", encoding="utf-8-sig") as f:
                dados = json.load(f)

            if not isinstance(dados, list):
                logger.error(
                    f"[SYNC] Blacklist inválida em {caminho}: esperado JSON do tipo lista, "
                    f"recebido {type(dados).__name__}."
                )
                self._blacklist_cache = set()
                self._blacklist_mtime = mtime_atual
                return self._blacklist_cache

            total_linhas_json = len(dados)

            blacklist = {
                pedido_id
                for item in dados
                if (pedido_id := self._normalizar_orderid(item))
            }

            duplicados_removidos = total_linhas_json - len(blacklist)

            self._blacklist_cache = blacklist
            self._blacklist_mtime = mtime_atual

            logger.info(
                f"[SYNC] Blacklist carregada: {len(blacklist)} pedidos únicos prontos para bloqueio. "
                f"Duplicados removidos do JSON: {duplicados_removidos}."
            )
            return self._blacklist_cache

        except json.JSONDecodeError as e:
            logger.error(f"[SYNC] JSON inválido na blacklist {caminho}: {e}")
        except Exception as e:
            logger.error(f"[SYNC] Falha ao ler a blacklist {caminho}: {e}", exc_info=True)

        self._blacklist_cache = set()
        return self._blacklist_cache

    # =========================================================
    # PREPARAÇÃO / VALIDAÇÃO DO PAYLOAD
    # =========================================================

    def _preparar_pedido(self, ped: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Enriquecimento seguro do pedido.
        Não altera o dicionário original.
        Retorna None se o pedido for inválido.
        """
        if not isinstance(ped, dict):
            logger.warning(f"[SYNC] Item inválido recebido no payload TOTVS: {ped!r}")
            return None

        orderid_norm = self._normalizar_orderid(ped.get("orderid"))
        if not orderid_norm:
            logger.warning(f"[SYNC] Pedido ignorado por orderid inválido: {ped!r}")
            return None

        issuedate_norm = self._normalizar_data(ped.get("issuedate"))
        if not issuedate_norm:
            logger.warning(
                f"[SYNC] Pedido {orderid_norm} ignorado por issuedate inválido: {ped.get('issuedate')!r}"
            )
            return None

        try:
            ano_str, mes_str, _ = issuedate_norm.split("-")
            ano_ref = int(ano_str)
            mes_ref = str(int(mes_str))  # remove zero à esquerda
        except Exception:
            logger.warning(
                f"[SYNC] Pedido {orderid_norm} ignorado por falha ao derivar período da data {issuedate_norm!r}"
            )
            return None

        pedido = dict(ped)
        pedido["orderid"] = orderid_norm
        pedido["orderid_normalizado"] = orderid_norm
        pedido["issuedate"] = issuedate_norm
        pedido["mes_referencia"] = mes_ref
        pedido["ano_referencia"] = ano_ref
        pedido["sellername"] = self._normalizar_texto(pedido.get("sellername")) or "DESCONHECIDO"
        pedido["sellerid"] = self._normalizar_texto(pedido.get("sellerid"))
        pedido["customername"] = self._normalizar_texto(pedido.get("customername"))
        pedido["amount"] = self._normalizar_amount(pedido.get("amount"))
        pedido["id_unico_linha"] = orderid_norm
        pedido["_hash"] = self._gerar_hash_pedido(pedido)

        return pedido

    def _filtrar_blacklist(self, pedidos: List[Dict[str, Any]], blacklist: Set[str]) -> List[Dict[str, Any]]:
        """
        Remove do payload todos os pedidos presentes na blacklist.
        """
        if not blacklist:
            return pedidos

        filtrados: List[Dict[str, Any]] = []
        total_bloqueados = 0

        for ped in pedidos:
            orderid = ped["orderid_normalizado"]
            if orderid in blacklist:
                total_bloqueados += 1
                logger.info(f"[SYNC] Pedido {orderid} bloqueado (consta na blacklist).")
                continue
            filtrados.append(ped)

        logger.info(
            f"[SYNC] Filtro blacklist aplicado | Recebidos: {len(pedidos)} | "
            f"Bloqueados: {total_bloqueados} | Liberados: {len(filtrados)}"
        )
        return filtrados

    def _deduplicar_lote_totvs(self, pedidos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicados dentro do próprio lote TOTVS usando orderid normalizado.
        Mantém a última ocorrência.
        """
        mapa: Dict[str, Dict[str, Any]] = {}
        duplicados = 0

        for ped in pedidos:
            orderid = ped["orderid_normalizado"]
            if orderid in mapa:
                duplicados += 1
                logger.warning(f"[SYNC] Pedido duplicado no payload TOTVS detectado: {orderid}. Mantendo a última ocorrência.")
            mapa[orderid] = ped

        if duplicados:
            logger.info(f"[SYNC] Deduplicação do lote TOTVS concluída. Duplicados removidos: {duplicados}.")

        return list(mapa.values())

    def _agrupar_pedidos(self, pedidos: List[Dict[str, Any]]) -> Dict[Tuple[str, str, int], List[Dict[str, Any]]]:
        """
        Agrupa por (vendedor, mês, ano).
        """
        grupos: Dict[Tuple[str, str, int], List[Dict[str, Any]]] = {}

        for ped in pedidos:
            vendedor = ped.get("sellername", "DESCONHECIDO")
            mes_ref = ped["mes_referencia"]
            ano_ref = ped["ano_referencia"]

            chave = (vendedor, mes_ref, ano_ref)
            grupos.setdefault(chave, []).append(ped)

        return grupos

    # =========================================================
    # ORQUESTRAÇÃO PRINCIPAL
    # =========================================================

    async def process_totvs_payload(self, pedidos_totvs: List[Dict[str, Any]]) -> None:
        """
        Orquestra o processamento completo do payload bruto do TOTVS.
        """
        start_time = time.perf_counter()
        logger.info("[SYNC] Iniciando processamento concorrente de payload TOTVS.")

        if not pedidos_totvs:
            logger.warning("[SYNC] Payload vazio recebido. Operação abortada.")
            return

        # 1) Preparação e validação
        pedidos_preparados: List[Dict[str, Any]] = []
        for ped in pedidos_totvs:
            preparado = self._preparar_pedido(ped)
            if preparado:
                pedidos_preparados.append(preparado)

        if not pedidos_preparados:
            logger.warning("[SYNC] Nenhum pedido válido restou após normalização/validação.")
            return

        # 2) Deduplicação do próprio lote TOTVS
        pedidos_preparados = self._deduplicar_lote_totvs(pedidos_preparados)

        # 3) Blacklist
        blacklist = self._carregar_blacklist()
        pedidos_preparados = self._filtrar_blacklist(pedidos_preparados, blacklist)

        if not pedidos_preparados:
            logger.warning("[SYNC] Todos os pedidos deste lote foram barrados pela blacklist.")
            return

        # 4) Upsert de vendedores fora do loop principal
        try:
            await asyncio.to_thread(self.client.upsert_vendedores, pedidos_preparados)
        except Exception as e:
            logger.error(f"[SYNC] Falha ao sincronizar vendedores: {e}", exc_info=True)

        # 5) Agrupamento
        grupos = self._agrupar_pedidos(pedidos_preparados)
        logger.info(f"[SYNC] Total de grupos para processamento: {len(grupos)}")

        # 6) Processamento concorrente com semáforo
        sem = asyncio.Semaphore(self.max_concurrent_groups)

        async def process_with_semaphore(
            vendedor: str,
            mes_ref: str,
            ano_ref: int,
            pedidos_grupo: List[Dict[str, Any]],
        ) -> None:
            async with sem:
                await self._process_sync_group(vendedor, mes_ref, ano_ref, pedidos_grupo)

        tasks = [
            asyncio.create_task(process_with_semaphore(vendedor, mes_ref, ano_ref, pedidos_grupo))
            for (vendedor, mes_ref, ano_ref), pedidos_grupo in grupos.items()
        ]

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"[SYNC] Uma task falhou: {result}", exc_info=True)

        duracao = time.perf_counter() - start_time
        logger.info(f"[SYNC] Sincronização do lote completo finalizada em {duracao:.2f}s")

    # =========================================================
    # PROCESSAMENTO DO GRUPO
    # =========================================================

    async def _process_sync_group(
        self,
        vendedor: str,
        mes_ref: str,
        ano_ref: int,
        pedidos_atuais: List[Dict[str, Any]],
    ) -> None:
        """
        Calcula delta e envia somente NOVOS e ATUALIZADOS.
        """
        start_time = time.perf_counter()
        log_id = None
        status_final = "ERRO"

        try:
            cache = await asyncio.to_thread(self.repo.get_cache_by_periodo, vendedor, mes_ref, ano_ref)
            if cache is None:
                cache = {}

            pedidos_payload: List[Dict[str, Any]] = []
            stats = {"new": 0, "upd": 0, "skip": 0}

            for ped in pedidos_atuais:
                id_l = ped["id_unico_linha"]
                curr_hash = ped["_hash"]
                cached_hash = cache.get(id_l)

                if not cached_hash:
                    ped_envio = dict(ped)
                    ped_envio["tipo_registro"] = "NOVO"
                    ped_envio["status"] = "ATIVO"
                    pedidos_payload.append(ped_envio)
                    stats["new"] += 1

                elif cached_hash != curr_hash:
                    ped_envio = dict(ped)
                    ped_envio["tipo_registro"] = "ATUALIZADO"
                    ped_envio["status"] = "ATIVO"
                    pedidos_payload.append(ped_envio)
                    stats["upd"] += 1

                else:
                    stats["skip"] += 1

            if not pedidos_payload:
                logger.info(
                    f"[DELTA] {vendedor} ({mes_ref}/{ano_ref}): sem alterações. "
                    f"Pulados: {stats['skip']}."
                )
                status_final = "SEM_ALTERACOES"
                return

            logger.info(
                f"[DELTA] {vendedor} ({mes_ref}/{ano_ref}): "
                f"{stats['new']} novos, {stats['upd']} atualizados, {stats['skip']} sem alteração."
            )

            # Cria log da importação
            log_id = await asyncio.to_thread(
                self.client.criar_log_importacao,
                vendedor,
                mes_ref,
                f"TOTVS_API_{vendedor}_{mes_ref}_{ano_ref}",
                len(pedidos_payload),
            )

            if not log_id:
                logger.error(f"[FAIL] {vendedor} | Não foi possível criar o log_id. Abortando envio do grupo.")
                status_final = "ERRO_LOG"
                return

            # Injeta importacao_id
            for p in pedidos_payload:
                p["importacao_id"] = log_id

            chunks = [
                pedidos_payload[i:i + self.max_batch_size]
                for i in range(0, len(pedidos_payload), self.max_batch_size)
            ]

            sucesso_total = True

            for indice, chunk in enumerate(chunks, start=1):
                logger.info(
                    f"[SYNC] {vendedor} | Enviando chunk {indice}/{len(chunks)} "
                    f"do período {mes_ref}/{ano_ref} com {len(chunk)} registros."
                )

                upsert_ok = await asyncio.to_thread(self.client.upsert_pedidos, chunk)

                if upsert_ok:
                    await self._persist_changes(chunk, vendedor, mes_ref, ano_ref)
                else:
                    sucesso_total = False
                    logger.error(
                        f"[FAIL] {vendedor} | Falha no envio do chunk {indice}/{len(chunks)} "
                        f"para {mes_ref}/{ano_ref}."
                    )
                    break

            status_final = "SUCESSO" if sucesso_total else "ERRO_PARCIAL"

            duracao = time.perf_counter() - start_time
            if sucesso_total:
                logger.info(f"[DONE] {vendedor} | Período {mes_ref}/{ano_ref} processado em {duracao:.2f}s")

        except Exception as e:
            logger.error(f"[FATAL] Erro no grupo {vendedor} ({mes_ref}/{ano_ref}): {e}", exc_info=True)
            status_final = "ERRO"

        finally:
            if log_id:
                try:
                    await asyncio.to_thread(self.client.finalizar_log_importacao, log_id, status_final)
                except Exception as e:
                    logger.error(f"[LOG] Falha ao finalizar log {log_id}: {e}", exc_info=True)

    # =========================================================
    # PERSISTÊNCIA DO CACHE LOCAL
    # =========================================================

    async def _persist_changes(
        self,
        vendas_chunk: List[Dict[str, Any]],
        vendedor: str,
        mes: str,
        ano: int,
    ) -> None:
        """
        Atualiza o cache local SQLite de forma transacional.
        """
        to_upsert = []
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

        for v in vendas_chunk:
            to_upsert.append(
                (
                    v["id_unico_linha"],
                    v.get("_hash", ""),
                    v.get("sellername", vendedor),
                    mes,
                    ano,
                    timestamp,
                )
            )

        if not to_upsert:
            return

        try:
            # Mantido o contrato atual: update_batch(to_delete, to_upsert)
            await asyncio.to_thread(self.repo.update_batch, [], to_upsert)
        except Exception as e:
            logger.error(f"[CACHE] Falha ao persistir alterações locais: {e}", exc_info=True)