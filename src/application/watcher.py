# src/application/watcher.py

# 1. Imports da Biblioteca Padrão
import os
import time
import re
from pathlib import Path
from typing import TYPE_CHECKING

# 2. Imports de Terceiros
from watchdog.events import FileSystemEventHandler, FileSystemEvent

# 3. Imports da Infraestrutura e Configuração
from src.config import CONFIG 
from src.infrastructure import logger, ExcelProcessor
from src.domain import DataSanitizer

# 4. Imports para Type Hinting
if TYPE_CHECKING:
    from src.application.sync import SyncService

# ==============================================================================
# WATCHDOG: EVENT HANDLER (CAMADA DE APLICAÇÃO)
# ==============================================================================

class SentinelEventHandler(FileSystemEventHandler):
    def __init__(self, sync_service: 'SyncService', vendedores_cache: dict):
        self.sync_service = sync_service
        self.vendedores_cache = vendedores_cache 
        self._debounce_cache = {}
        self._debounce_seconds = 60 
        
        # Regex para ignorar arquivos temporários e de sistema
        self.ignore_patterns = re.compile(
            r'(~\$|^\.|^__|\.tmp$|\.temp$|\.crdownload$|desktop\.ini)', 
            re.IGNORECASE
        )

        # Mapa de definição dos padrões Regex para cada mês
        mapa_meses = {
            1: r'01_JANEIRO', 2: r'02_FEVEREIRO', 3: r'03_MAR[CÇ]O',
            4: r'04_ABRIL', 5: r'05_MAIO', 6: r'06_JUNHO',
            7: r'07_JULHO', 8: r'08_AGOSTO', 9: r'09_SETEMBRO',
            10: r'10_OUTUBRO', 11: r'11_NOVEMBRO', 12: r'12_DEZEMBRO'
        }

        # Lógica de Whitelist (Respeitando o .env)
        if CONFIG.TARGET_MONTH and CONFIG.TARGET_MONTH in mapa_meses:
            padrao_mes = mapa_meses[CONFIG.TARGET_MONTH]
            logger.info(f"🎯 [WATCHER] Modo Focado: Aceitando APENAS '{padrao_mes}'")
        else:
            padrao_mes = "|".join(mapa_meses.values())
            logger.info("👀 [WATCHER] Modo Global: Monitorando todos os meses fiscais.")

        self.whitelist_pattern = re.compile(
            rf'^({padrao_mes})_RELATORIO-DE-VENDAS\.xlsx?$',
            re.IGNORECASE | re.VERBOSE
        )

    def on_modified(self, event: FileSystemEvent):
        self._filter_and_route(event)

    def on_created(self, event: FileSystemEvent):
        self._filter_and_route(event)

    def _filter_and_route(self, event: FileSystemEvent):
        """Aplica filtros de segurança e logs de detecção inicial."""
        if event.is_directory:
            return

        file_path = Path(event.src_path).absolute()
        file_name = file_path.name

        # 1. Filtros Básicos (Extensão e Temporários)
        if not file_name.lower().endswith(('.xlsx', '.xls')) or self.ignore_patterns.search(file_name):
            return

        # 2. Filtro de Nome Oficial (Whitelist)
        if not self.whitelist_pattern.match(file_name):
            return

        # 3. Validação Física
        if not file_path.exists():
            return

        # 4. Debounce (Evita processar o mesmo salvamento várias vezes)
        now = time.time()
        path_str = str(file_path)
        if (now - self._debounce_cache.get(path_str, 0)) < self._debounce_seconds:
            return
        self._debounce_cache[path_str] = now

        # [LOG DE DETECÇÃO] - Primeiro contato com o arquivo
        logger.info(f"🔔 [EVENTO] Arquivo oficial detectado: {file_name}")
        
        # Delay de estabilização (Essencial para redes Z: e Y:)
        time.sleep(2)
        self._handle_file(path_str)

    def _handle_file(self, file_path_str: str):
        """Valida a 'identidade' da pasta (Nível Vendedor) e orquestra o processamento."""
        path_obj = Path(file_path_str)
        file_name = path_obj.name
        
        # --- CORREÇÃO: LÓGICA DE "AVÓ" (GRANDPARENT) ---
        # Estrutura:  Z:\Vendas\DIEGO ARANTES\01_JANEIRO\Arquivo.xlsx
        # Parent (Mãe): 01_JANEIRO
        # Parent.Parent (Avó): DIEGO ARANTES <--- É aqui que está o nome!
        
        try:
            nome_pasta_avo = path_obj.parent.parent.name
        except IndexError:
            # Proteção caso alguém salve na raiz (Z:\Arquivo.xlsx)
            nome_pasta_avo = "DESCONHECIDO"

        # Normaliza (DIEGO_ARANTES -> DIEGO ARANTES) para bater com o banco
        vendedor_pasta = DataSanitizer.normalize_name(nome_pasta_avo)

        # --- TRIPLE MATCH: SEGURANÇA DE PASTA ---
        if vendedor_pasta not in self.vendedores_cache:
            logger.warning(f"🚫 [SEGURANÇA] A pasta '{nome_pasta_avo}' não é de um vendedor autorizado.")
            logger.warning(f"   Caminho: {file_path_str}")
            logger.warning(f"   Ação: IGNORADO (Salve dentro da pasta do Vendedor -> Mês).")
            return
        
        logger.info(f"📂 [DIRETÓRIO] Pasta validada: {vendedor_pasta} | Processando: {file_name}")
        
        try:
            # Passamos o cache de vendedores para o processor validar a linha 6 também
            processor = ExcelProcessor(file_path_str, self.vendedores_cache)
            
            if not processor.wait_for_lock_release():
                logger.warning(f"⏳ [TIMEOUT] O Excel ainda está prendendo o arquivo: {file_name}")
                return

            # Extração
            resultado = processor.parse()
            if not resultado:
                return

            vendedor_planilha, vendas = resultado

            # Log de confirmação de Identidade (Pasta vs Planilha)
            if vendedor_pasta != vendedor_planilha:
                # Normaliza a planilha também para evitar falsos positivos (DIEGO  ARANTES vs DIEGO ARANTES)
                vend_plan_norm = DataSanitizer.normalize_name(vendedor_planilha)
                if vendedor_pasta != vend_plan_norm:
                    logger.warning(f"⚠️  [DIVERGÊNCIA] Arquivo salvo na pasta de '{vendedor_pasta}', "
                                f"mas a planilha diz ser de '{vendedor_planilha}'")

            # Envia para a Sincronização
            self.sync_service.process_sync(vendedor_planilha, vendas)

        except PermissionError:
            logger.error(f"🚫 [ACESSO] Arquivo travado pelo Excel (PermissionError): {file_name}")
        except Exception as e:
            logger.error(f"💥 [ERRO] Falha crítica no monitoramento de {file_name}: {str(e)}", exc_info=True)

        # Limpeza periódica do cache de debounce
        if len(self._debounce_cache) > 500:
            self._debounce_cache.clear()