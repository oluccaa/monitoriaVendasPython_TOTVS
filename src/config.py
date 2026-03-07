# src/config.py

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

def _safe_int(value: Optional[str]) -> Optional[int]:
    """Converte para inteiro com seguranca, prevenindo crash por strings vazias."""
    if value and str(value).strip().isdigit():
        return int(str(value).strip())
    return None

@dataclass(frozen=True)
class AppConfig:
    APP_NAME: str = "AcosVital_TOTVS_Sync"
    VERSION: str = "13.1.0"
    
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    DB_PATH: Path = BASE_DIR / "data" / "totvs_cache.db"
    LOG_DIR: Path = BASE_DIR / "logs"
    LOG_FILE: Path = LOG_DIR / "totvs_sync.log"
    
    TARGET_MONTH: Optional[int] = _safe_int(os.getenv("TARGET_MONTH"))
    TARGET_YEAR: Optional[int] = _safe_int(os.getenv("TARGET_YEAR"))

    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    
    TOTVS_URL: str = os.getenv("TOTVS_URL", "")
    TOTVS_USER: str = os.getenv("TOTVS_USER", "")
    TOTVS_PASS: str = os.getenv("TOTVS_PASS", "")

    TIMEOUT_REQUEST: int = _safe_int(os.getenv("API_TIMEOUT")) or 45
    POLLING_INTERVAL: int = _safe_int(os.getenv("POLLING_INTERVAL")) or 60

    def __post_init__(self):
        try:
            self.LOG_DIR.mkdir(parents=True, exist_ok=True)
            self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"[ERRO CRITICO] Falha ao criar estrutura de diretorios: {e}")

CONFIG = AppConfig()