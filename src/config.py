# src/config.py

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    # --- Identidade ---
    APP_NAME: str = "AcosVital_TOTVS_Sync"
    VERSION: str = "13.0.0"
    
    # --- Caminhos Internos ---
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    DB_PATH: Path = BASE_DIR / "data" / "totvs_cache.db"
    LOG_DIR: Path = BASE_DIR / "logs"
    LOG_FILE: Path = LOG_DIR / "totvs_sync.log"
    
    # --- Filtros de Execucao (Opcionais) ---
    TARGET_MONTH: int = int(os.getenv("TARGET_MONTH")) if os.getenv("TARGET_MONTH") else None
    TARGET_YEAR: int = int(os.getenv("TARGET_YEAR")) if os.getenv("TARGET_YEAR") else None

    # --- Conexao Supabase ---
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    
    # --- Conexao TOTVS Protheus ---
    TOTVS_URL: str = os.getenv("TOTVS_URL", "")
    TOTVS_USER: str = os.getenv("TOTVS_USER", "")
    TOTVS_PASS: str = os.getenv("TOTVS_PASS", "")

    # --- Configuracoes de Rede e Polling ---
    TIMEOUT_REQUEST: int = int(os.getenv("API_TIMEOUT", 45))
    POLLING_INTERVAL: int = int(os.getenv("POLLING_INTERVAL", 60))

    def __post_init__(self):
        try:
            self.LOG_DIR.mkdir(parents=True, exist_ok=True)
            self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Erro critico ao criar pastas base: {e}")

CONFIG = AppConfig()