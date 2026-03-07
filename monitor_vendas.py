# monitor_vendas.py

import sys
import time
import os
from pathlib import Path

# ==============================================================================
# IMPORTACOES DA NOVA ARQUITETURA (DDD)
# ==============================================================================
try:
    # 1. Configuracao Global
    from src.config import CONFIG
    
    # 2. Infraestrutura (Ferramentas Tecnicas)
    from src.infrastructure.logging import logger
    from src.infrastructure.database import DatabaseRepository
    from src.infrastructure.supabase_client import SupabaseRepository
    from src.infrastructure.totvs_client import TOTVSClient
    
    # 3. Aplicacao (Orquestracao)
    from src.application.sync import SyncService
    from src.application.poller import TOTVSPoller

except ImportError as e:
    print(f"[ERRO CRITICO DE IMPORTACAO] {e}")
    print("Certifique-se de que instalou as dependencias: pip install python-dotenv requests supabase")
    print("Verifique se a pasta 'src' contem os arquivos __init__.py.")
    sys.exit(1)

# ==============================================================================
# MAIN & BOOTSTRAP
# ==============================================================================

def run_system():
    """Inicializacao com Injecao de Dependencias e Resiliencia."""
    
    logger.info("===================================================")
    logger.info(f"   [{CONFIG.APP_NAME.upper()}] - INICIANDO SISTEMA   ")
    logger.info("===================================================")

    # 1. Inicializacao de Infraestrutura
    try:
        # Garante que o diretorio do banco de dados existe antes de conectar
        CONFIG.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        repo = DatabaseRepository(CONFIG.DB_PATH)
        logger.info(f"[SQLITE] Banco local iniciado em: {CONFIG.DB_PATH}")
    except Exception as e:
        logger.critical(f"[FATAL] Erro ao conectar no cache local: {e}")
        return

    # --- CONEXAO SUPABASE ---
    try:
        # O cliente Supabase sera usado para persistencia e logs de importacao
        client = SupabaseRepository()
        logger.info("[SUPABASE] Cliente configurado e conectado.")
    except Exception as e:
        logger.critical(f"[FATAL] Erro ao conectar no Supabase: {e}")
        return

    # --- CONEXAO TOTVS ---
    try:
        # Cliente REST para consumo do endpoint da Acos Vital
        totvs_client = TOTVSClient(
            base_url=CONFIG.TOTVS_URL,
            username=CONFIG.TOTVS_USER,
            password=CONFIG.TOTVS_PASS
        )
        logger.info("[TOTVS] Cliente REST configurado com sucesso.")
    except Exception as e:
        logger.critical(f"[FATAL] Erro ao configurar cliente TOTVS: {e}")
        return

    # 2. Inicializacao dos Servicos
    # O SyncService gerencia a lógica de Delta (Novos vs Existentes)
    sync_service = SyncService(repo, client)
    
    # O Poller gerencia o intervalo de tempo entre as buscas na API
    poller = TOTVSPoller(
        totvs_client=totvs_client,
        sync_service=sync_service,
        interval_seconds=CONFIG.POLLING_INTERVAL
    )

    # ==========================================================================
    # 3. EXECUCAO DO LOOP PRINCIPAL
    # ==========================================================================
    try:
        logger.info("[SISTEMA] Inicializacao concluida. Iniciando motor de Polling...")
        poller.start()
        
    except KeyboardInterrupt:
        logger.info("[SISTEMA] Desligamento solicitado pelo usuario via teclado.")
        poller.stop()
    except Exception as e:
        logger.critical(f"[SISTEMA] Erro catastrófico no loop principal: {e}", exc_info=True)
        poller.stop()
    finally:
        logger.info("[SISTEMA] Processo encerrado de forma segura.")

if __name__ == "__main__":
    # Customiza o titulo da janela no Windows para facilitar identificacao
    if os.name == 'nt':
        os.system(f"title {CONFIG.APP_NAME}")
    
    # Delay util para aguardar rede em caso de boot automatico do Windows
    if "--boot" in sys.argv:
        logger.info("[BOOT] Aguardando estabilizacao de rede (30s)...")
        time.sleep(30)
        
    run_system()