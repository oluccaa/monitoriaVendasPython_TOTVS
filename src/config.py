# src/config.py

import os
from typing import Dict, Tuple, Optional
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    # --- Identidade ---
    APP_NAME: str = "VilaVendas Sentinel v12.0 - Supabase Direct"
    VERSION: str = "12.0.0"
    
    # --- Caminhos Internos ---
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    DB_PATH: Path = BASE_DIR / "data" / "sentinel_cache.db"
    LOG_DIR: Path = BASE_DIR / "logs"
    LOG_FILE: Path = LOG_DIR / "sentinel.log"
    
    # --- Filtros de Execução ---
    TARGET_MONTH: Optional[int] = int(os.getenv("TARGET_MONTH")) if os.getenv("TARGET_MONTH") else None
    TARGET_YEAR: Optional[int] = int(os.getenv("TARGET_YEAR")) if os.getenv("TARGET_YEAR") else None

    # --- Conexão Supabase (Direta) ---
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    
    # Configurações de Retentativa
    TIMEOUT_REQUEST: int = int(os.getenv("API_TIMEOUT", 30))

    # --- Monitoramento (Pastas de Rede) ---
    PASTAS_MONITORADAS: Tuple[str, ...] = field(default_factory=lambda: (
        os.getenv("PATH_Z", r"Z:\Vendas_Acos-Vital\Vendas"), 
        os.getenv("PATH_Y", r"Y:\Vendas_Acos-Vital\Vendas")
    ))

    # --- Auditoria (Controladoria) ---
    PATH_AUDITORIA: Path = Path(r"Z:\CONTROLADORIA")
    ARQUIVO_MANIFESTO: Path = PATH_AUDITORIA / "pedidos_remover.xlsx" 
    ARQUIVO_VENDEDORES: Path = PATH_AUDITORIA / "mapeamento_vendedores.xlsx"

    # Credenciais Omie (Adicione isto ao seu .env também)
    OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
    OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")
    
    # Configurações de Auditoria
    AUDIT_ENABLED = True

    # --- Mapeamento de Colunas ---
    EXCEL_MAP: Dict[str, int] = field(default_factory=lambda: {
        "DATA": 0, "PV": 1, "CLIENTE": 2, "VALOR_PEDIDO": 3,
        "VALOR_PENDENTE": 7, "VALOR_COMISSAO": 10
    })

    def __post_init__(self):
        try:
            self.LOG_DIR.mkdir(parents=True, exist_ok=True)
            self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"⚠️ Erro ao criar pastas: {e}")

    # --- Propriedades Atalho ---
    @property
    def COL_DATA(self) -> int: return self.EXCEL_MAP["DATA"]
    @property
    def COL_PV(self) -> int: return self.EXCEL_MAP["PV"]
    @property
    def COL_CLIENTE(self) -> int: return self.EXCEL_MAP["CLIENTE"]
    @property
    def COL_VALOR_PEDIDO(self) -> int: return self.EXCEL_MAP["VALOR_PEDIDO"]
    @property
    def COL_VALOR_PENDENTE(self) -> int: return self.EXCEL_MAP["VALOR_PENDENTE"]
    @property
    def COL_VALOR_COMISSAO(self) -> int: return self.EXCEL_MAP["VALOR_COMISSAO"]

CONFIG = AppConfig()