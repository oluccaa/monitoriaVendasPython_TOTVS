# monitor_vendas.py

import sys
import time
import os
import signal
from pathlib import Path

# ==============================================================================
# CONFIGURAÇÃO DE CAMINHOS E IMPORTAÇÕES
# ==============================================================================
# Garante que a raiz do projeto esteja no path para evitar erros de importação
BASE_DIR = Path(__file__).parent.resolve()
sys.path.append(str(BASE_DIR))

try:
    from src.config import CONFIG
    from src.infrastructure.logging import logger
    from src.infrastructure.database import DatabaseRepository
    from src.infrastructure.supabase_client import SupabaseRepository
    from src.infrastructure.totvs_client import TOTVSClient
    from src.application.sync import SyncService
    from src.application.poller import TOTVSPoller

except ImportError as e:
    print(f"[ERRO CRITICO DE IMPORTACAO] {e}")
    print("Dica: Verifique se as pastas 'src', 'src/infrastructure' e 'src/application'")
    print("possuem o arquivo __init__.py e se as dependencias estao instaladas.")
    sys.exit(1)

# ==============================================================================
# GESTÃO DE DESLIGAMENTO (GRACEFUL SHUTDOWN)
# ==============================================================================
poller_instance = None

def handle_exit_signal(sig, frame):
    """Captura sinais de interrupção (Ctrl+C, encerramento do sistema)"""
    global poller_instance
    if poller_instance:
        logger.info("[SISTEMA] Sinal de desligamento recebido. Parando motor...")
        poller_instance.stop()
    sys.exit(0)

# Registra os sinais de interrupção do sistema
signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)

# ==============================================================================
# VALIDAÇÃO DE CONFIGURAÇÃO
# ==============================================================================
def validate_config():
    """Verifica se as variáveis essenciais estão preenchidas no .env"""
    missing = []
    if not CONFIG.TOTVS_URL: missing.append("TOTVS_URL")
    if not CONFIG.SUPABASE_URL: missing.append("SUPABASE_URL")
    if not CONFIG.SUPABASE_KEY: missing.append("SUPABASE_KEY")
    
    if missing:
        logger.critical(f"[CONFIG] Erro: Faltam as seguintes variaveis no .env: {', '.join(missing)}")
        return False
    return True

# ==============================================================================
# MAIN & BOOTSTRAP
# ==============================================================================

def run_system():
    """Inicializacao com Injecao de Dependencias e Resiliencia."""
    global poller_instance
    
    logger.info("===================================================")
    logger.info(f"   [{CONFIG.APP_NAME.upper()}] - INICIANDO SISTEMA   ")
    logger.info("===================================================")

    if not validate_config():
        return

    # 1. Inicializacao de Infraestrutura
    try:
        # Garante que o diretorio do banco de dados existe
        CONFIG.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        repo = DatabaseRepository(CONFIG.DB_PATH)
        logger.info(f"[SQLITE] Banco local iniciado em: {CONFIG.DB_PATH}")
    except Exception as e:
        logger.critical(f"[FATAL] Erro ao conectar no cache local: {e}")
        return

    # --- CONEXAO SUPABASE ---
    try:
        client = SupabaseRepository()
        logger.info("[SUPABASE] Cliente configurado e conectado com sucesso.")
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
        logger.info("[TOTVS] Cliente REST configurado e validado.")
    except Exception as e:
        logger.critical(f"[FATAL] Erro ao configurar cliente TOTVS: {e}")
        return

    # 2. Inicializacao dos Servicos
    sync_service = SyncService(repo, client)
    
    poller_instance = TOTVSPoller(
        totvs_client=totvs_client,
        sync_service=sync_service,
        interval_seconds=CONFIG.POLLING_INTERVAL
    )

    # ==========================================================================
    # 3. EXECUCAO DO LOOP PRINCIPAL
    # ==========================================================================
    try:
        logger.info(f"[SISTEMA] Polling a cada {CONFIG.POLLING_INTERVAL}s. Iniciando motor...")
        poller_instance.start()
        
    except Exception as e:
        logger.critical(f"[SISTEMA] Erro inesperado no loop principal: {e}", exc_info=True)
        if poller_instance:
            poller_instance.stop()
    finally:
        logger.info("[SISTEMA] Processo finalizado.")

if __name__ == "__main__":
    # Customiza o titulo da janela no Windows
    if os.name == 'nt':
        os.system(f"title {CONFIG.APP_NAME}")
    
    # Delay util para aguardar rede em caso de boot automatico
    if "--boot" in sys.argv:
        logger.info("[BOOT] Aguardando estabilizacao de rede (30s)...")
        time.sleep(30)
        
    run_system()