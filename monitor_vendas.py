# monitor_vendas.py

# 1. Imports da Biblioteca Padrao
import sys
import time
import os

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
    
    # Assumindo que voce renomeou o watcher.py para poller.py conforme discutimos
    from src.application.poller import TOTVSPoller

except ImportError as e:
    print(f"[ERRO CRITICO DE IMPORTACAO] {e}")
    print("Verifique se a pasta 'src' existe e tem os arquivos configurados corretamente.")
    print("Dica: Execute este script da raiz do projeto (onde esta o arquivo .env).")
    sys.exit(1)

# ==============================================================================
# MAIN & BOOTSTRAP
# ==============================================================================

def run_system():
    """Inicializacao com Injeção de Dependencias e Resiliencia."""
    
    logger.info("===================================================")
    logger.info(f"   [{CONFIG.APP_NAME.upper()}] - INICIANDO SISTEMA   ")
    logger.info("===================================================")

    # 1. Inicializacao de Infraestrutura
    try:
        repo = DatabaseRepository(CONFIG.DB_PATH)
        logger.info("[SQLITE] Banco local de cache iniciado com sucesso.")
    except Exception as e:
        logger.critical(f"[FATAL] Erro ao conectar no cache local: {e}")
        return

    # --- CONEXAO SUPABASE ---
    try:
        client = SupabaseRepository()
        logger.info("[SUPABASE] Cliente conectado com sucesso.")
    except Exception as e:
        logger.critical(f"[FATAL] Erro ao conectar no Supabase: {e}")
        return

    # --- CONEXAO TOTVS ---
    try:
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
    sync_service = SyncService(repo, client)
    
    # O Poller assume o papel do antigo SentinelEventHandler/Observer
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
        # O metodo start() contem o laco while True e bloqueara a execucao aqui
        poller.start() 
        
    except KeyboardInterrupt:
        logger.info("[SISTEMA] Desligamento solicitado pelo usuario via teclado.")
        poller.stop()
    except Exception as e:
        logger.critical(f"[SISTEMA] Erro catastrofico no loop principal: {e}", exc_info=True)
        poller.stop()
    finally:
        logger.info("[SISTEMA] Processo encerrado. Ate logo.")

if __name__ == "__main__":
    # Customiza o titulo da janela no Windows
    if os.name == 'nt':
        os.system(f"title {CONFIG.APP_NAME}")
    
    # Parametro util para inicializacao via agendador de tarefas do Windows (Task Scheduler)
    if "--boot" in sys.argv:
        logger.info("[BOOT] Aguardando estabilizacao de rede do sistema operacional (30s)...")
        time.sleep(30)
        
    run_system()