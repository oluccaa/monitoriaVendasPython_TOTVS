# src/infrastructure/logging.py

# 1. Imports da Biblioteca Padrao
import logging
import sys
from logging.handlers import RotatingFileHandler

# 2. Imports da Aplicacao
from src.config import CONFIG

# ==============================================================================
# INFRAESTRUTURA: SISTEMA DE LOGS TOTVS
# ==============================================================================

class CustomFormatter(logging.Formatter):
    """
    Formatador personalizado para adicionar cores ao terminal.
    Ajuda a distinguir visualmente Erros (Vermelho) de Informacoes (Verde).
    """
    
    gray = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    green = "\x1b[32;20m"
    reset = "\x1b[0m"
    
    # Formato rigoroso com precisao de milissegundos para cronometrar cada etapa
    fmt = "[%(asctime)s.%(msecs)03d] | %(levelname)-8s | (%(filename)s:%(lineno)d) | %(message)s"

    FORMATS = {
        logging.DEBUG: gray + fmt + reset,
        logging.INFO: green + fmt + reset,
        logging.WARNING: yellow + fmt + reset,
        logging.ERROR: red + fmt + reset,
        logging.CRITICAL: bold_red + fmt + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        # Data no formato Ano-Mes-Dia Hora:Minuto:Segundo para facilitar auditoria
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

def setup_logger() -> logging.Logger:
    """
    Configura e retorna uma instancia unica do logger.
    Configura rotacao de arquivos (20MB) e saida colorida no console.
    """
    
    # Renomeado para refletir o novo sistema
    logger = logging.getLogger("AcosVital_TOTVS")
    logger.setLevel(logging.DEBUG)
    
    # Limpa handlers anteriores para evitar logs duplicados se a funcao for chamada novamente
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Criacao da pasta de logs (Seguranca)
    try:
        CONFIG.LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"ERRO CRITICO: Nao foi possivel criar pasta de logs: {e}")

    # 2. FILE HANDLER (Arquivo de Texto)
    # Rotacao de 20MB, mantem os ultimos 10 arquivos
    try:
        # Formato de arquivo tambem com milissegundos para rastreabilidade de tempo exato
        file_formatter = logging.Formatter(
            fmt='[%(asctime)s.%(msecs)03d] | %(levelname)-8s | [%(funcName)s] %(message)s',
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
        print(f"AVISO: Nao foi possivel configurar log em arquivo: {e}")

    # 3. CONSOLE HANDLER (Terminal Colorido)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(CustomFormatter())
    console_handler.setLevel(logging.DEBUG) # No terminal, mostramos tudo
    logger.addHandler(console_handler)

    return logger

# Instancia Singleton
# Ao fazer "from src.infrastructure.logging import logger", voce recebe esta instancia pronta.
logger = setup_logger()