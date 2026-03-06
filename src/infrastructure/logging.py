# src/infrastructure/logging.py

# 1. Imports da Biblioteca Padrão
import logging
import sys
from logging.handlers import RotatingFileHandler

# 2. Imports da Aplicação
from src.config import CONFIG

# ==============================================================================
# INFRAESTRUTURA: SISTEMA DE LOGS
# ==============================================================================

class CustomFormatter(logging.Formatter):
    """
    Formatador personalizado para adicionar cores ao terminal.
    Ajuda a distinguir visualmente Erros (Vermelho) de Informações (Verde).
    """
    
    gray = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    green = "\x1b[32;20m"
    reset = "\x1b[0m"
    
    # Formato: Hora | Nível | (Arquivo:Linha) | Mensagem
    fmt = "%(asctime)s | %(levelname)-8s | (%(filename)s:%(lineno)d) | %(message)s"

    FORMATS = {
        logging.DEBUG: gray + fmt + reset,
        logging.INFO: green + fmt + reset,
        logging.WARNING: yellow + fmt + reset,
        logging.ERROR: red + fmt + reset,
        logging.CRITICAL: bold_red + fmt + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        # Data no formato Hora:Minuto:Segundo
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

def setup_logger() -> logging.Logger:
    """
    Configura e retorna uma instância única do logger.
    Configura rotação de arquivos (20MB) e saída colorida no console.
    """
    
    logger = logging.getLogger(CONFIG.APP_NAME)
    logger.setLevel(logging.DEBUG)
    
    # Limpa handlers anteriores para evitar logs duplicados se a função for chamada novamente
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Criação da pasta de logs (Segurança)
    try:
        CONFIG.LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"ERRO CRÍTICO: Não foi possível criar pasta de logs: {e}")

    # 2. FILE HANDLER (Arquivo de Texto)
    # Rotação de 20MB, mantém os últimos 10 arquivos
    try:
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | [%(funcName)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler = RotatingFileHandler(
            CONFIG.LOG_FILE, 
            maxBytes=20*1024*1024, 
            backupCount=10, 
            encoding='utf-8',
            delay=True # Cria o arquivo apenas na primeira escrita
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO) # No arquivo, guardamos apenas o essencial
        logger.addHandler(file_handler)
        
    except Exception as e:
        print(f"⚠️ Aviso: Não foi possível configurar log em arquivo: {e}")

    # 3. CONSOLE HANDLER (Terminal Colorido)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(CustomFormatter())
    console_handler.setLevel(logging.DEBUG) # No terminal, mostramos tudo
    logger.addHandler(console_handler)

    return logger

# Instância Singleton
# Ao fazer "from src.infrastructure.logging import logger", você recebe esta instância pronta.
logger = setup_logger()