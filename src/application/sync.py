# src/application/sync.py

# 1. Imports da Biblioteca Padrão
import time
from typing import List, Dict, Any, TYPE_CHECKING

# 2. Imports da Infraestrutura
from src.infrastructure import logger
from src.config import CONFIG

# Uso de TYPE_CHECKING para evitar erros de importação circular
if TYPE_CHECKING:
    from src.infrastructure import DatabaseRepository
    from src.infrastructure.supabase_client import SupabaseRepository

# ==============================================================================
# SERVIÇO: SINCRONIZAÇÃO (PARTICIONAMENTO TEMPORAL)
# ==============================================================================

class SyncService:
    def __init__(self, repo: 'DatabaseRepository', client: 'SupabaseRepository'):
        self.repo = repo
        self.client = client
        self.max_batch_size = 200 

    def process_sync(self, vendedor: str, vendas_atuais: List[Dict[str, Any]]):
        """
        Executa a lógica de Delta com SEGURANÇA DE PERÍODO e logs profissionais.
        Utiliza TARGET_MONTH e TARGET_YEAR do CONFIG (env) como fallback de segurança.
        """
        start_time = time.perf_counter()
        
        try:
            # 1. SEGURANÇA: Identifica o Período do Arquivo
            if not vendas_atuais:
                logger.warning(f"⚠️  [SKIP] {vendedor} | Arquivo lido mas sem dados válidos para processar.")
                return

            # Prioriza os dados reais do arquivo, mas usa o .env (CONFIG) como rede de segurança
            mes_ref = str(vendas_atuais[0].get('mes_referencia', CONFIG.TARGET_MONTH or 'DESCONHECIDO'))
            ano_ref = vendas_atuais[0].get('ano_referencia', CONFIG.TARGET_YEAR or 2026)

            # [LOG INICIAL] Identidade e Tempo unidos no início da transação
            logger.info(f"🚀 [INIT] {vendedor} | Iniciando sincronização para o período {mes_ref}/{ano_ref}")
            
            # 2. CACHE PARTICIONADO
            # Garante isolamento: processar um mês nunca afetará os dados de outro mês.
            cache = self.repo.get_cache_by_periodo(vendedor, mes_ref, ano_ref)
            
            vendas_payload = []
            ids_na_planilha = set()
            stats = {"new": 0, "upd": 0, "del": 0}

            # 3. Delta Positivo (Inserções e Atualizações)
            for venda in vendas_atuais:
                id_l = venda["id_unico_linha"]
                curr_hash = venda["_hash"]
                ids_na_planilha.add(id_l)

                cached_hash = cache.get(id_l)

                if not cached_hash:
                    venda["tipo_registro"] = "NOVO"
                    venda["status"] = "ATIVO"
                    vendas_payload.append(venda)
                    stats["new"] += 1
                elif cached_hash != curr_hash:
                    venda["tipo_registro"] = "ATUALIZADO"
                    venda["status"] = "ATIVO"
                    vendas_payload.append(venda)
                    stats["upd"] += 1

            # 4. Delta Negativo (Exclusões Lógicas de segurança por período)
            ids_deletados = set(cache.keys()) - ids_na_planilha
            
            for id_del in ids_deletados:
                vendas_payload.append({
                    "id_unico_linha": id_del,
                    "vendedor_nome_origem": vendedor,
                    "mes_referencia": mes_ref, 
                    "ano_referencia": ano_ref,
                    "status": "DELETADO",
                    "tipo_registro": "EXCLUSAO_LOGICA",
                    "_hash": "DELETADO"
                })
                stats["del"] += 1

            # 5. Verificação de Ociosidade (Evita logs desnecessários se nada mudou)
            if not vendas_payload:
                logger.info(f"⏭️  [IDLE] {vendedor} | {mes_ref}/{ano_ref} já está atualizado no banco.")
                return

            # [LOG DE STATUS] Resumo claro do que será enviado para a nuvem
            logger.info(f"📊 [DELTA] {vendedor} ({mes_ref}/{ano_ref}) -> "
                        f"+{stats['new']} Novos | ~{stats['upd']} Updates | -{stats['del']} Removidos")

            # ==================================================================
            # 6. ORQUESTRAÇÃO SUPABASE
            # ==================================================================
            
            # [PASSO A] Registro da Importação
            log_id = self.client.criar_log_importacao(
                vendedor=vendedor,
                mes=mes_ref,
                arquivo=f"{vendedor}_{mes_ref}_{ano_ref}.xlsx",
                qtd=len(vendas_payload)
            )

            if log_id:
                for v in vendas_payload:
                    v["importacao_id"] = log_id

            # [PASSO B] Envio em Lotes
            chunks = [vendas_payload[i:i + self.max_batch_size] 
                      for i in range(0, len(vendas_payload), self.max_batch_size)]
            
            total_chunks = len(chunks)
            chunks_sucesso = 0
            sucesso_total = True

            for i, chunk in enumerate(chunks, 1):
                if self.client.upsert_lote_vendas(chunk):
                    # Grava no banco local APENAS se o Supabase confirmou o sucesso do lote
                    self._persist_changes(chunk, mes_ref, ano_ref)
                    chunks_sucesso += 1
                else:
                    sucesso_total = False
                    logger.error(f"❌ [FAIL] {vendedor} | Falha no lote {i}/{total_chunks}. Operação interrompida.")
                    break 

            # [PASSO C] Finalização da Transação
            status_final = "SUCESSO" if sucesso_total else "ERRO_PARCIAL"
            self.client.finalizar_log_importacao(log_id, status_final)

            # [LOG FINAL] Conclusão com tempo total
            duracao = time.perf_counter() - start_time
            if sucesso_total:
                logger.info(f"✅ [DONE] {vendedor} | {mes_ref}/{ano_ref} finalizado em {duracao:.2f}s")

        except Exception as e:
            # Garante que o erro carregue a identidade do processo que falhou
            logger.error(f"💥 [FATAL] {vendedor} ({CONFIG.TARGET_MONTH}/{CONFIG.TARGET_YEAR}) | "
                         f"Erro crítico: {str(e)}", exc_info=True)

    def _persist_changes(self, vendas_chunk: List[Dict[str, Any]], mes: str, ano: int):
        """
        Mantém o cache local atualizado de forma transacional.
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
                    v.get("vendedor_nome_origem", "DESCONHECIDO"),
                    mes,
                    ano,
                    timestamp
                ))
        
        if to_delete or to_upsert:
            try:
                self.repo.update_batch(to_delete, to_upsert)
            except Exception as e:
                logger.error(f"💾 [CACHE ERROR] Falha ao persistir alterações locais: {e}")