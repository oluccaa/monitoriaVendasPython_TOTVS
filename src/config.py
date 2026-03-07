# src/config.py

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

# Carrega as variaveis do arquivo .env localizado na raiz do projeto
load_dotenv()

def _safe_int(value: Optional[str]) -> Optional[int]:
    """
    Converte para inteiro com seguranca.
    Evita falhas se a variavel de ambiente estiver vazia ou contiver texto invalido.
    """
    if value and str(value).strip().isdigit():
        return int(str(value).strip())
    return None

@dataclass(frozen=True)
class AppConfig:
    """
    Configuracao Centralizada da Aplicacao.
    Define caminhos, credenciais e parametros de execucao.
    """
    # --- Identidade do Sistema ---
    APP_NAME: str = "AcosVital_TOTVS_Sync"
    VERSION: str = "13.1.0"
    
    # --- Gestao de Caminhos (Paths) ---
    # Resolve o caminho absoluto da raiz do projeto a partir deste arquivo
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    DB_PATH: Path = BASE_DIR / "data" / "totvs_cache.db"
    LOG_DIR: Path = BASE_DIR / "logs"
    LOG_FILE: Path = LOG_DIR / "totvs_sync.log"
    
    # --- Filtros de Sincronizacao ---
    # Se definidos no .env, o sistema foca apenas no periodo especificado
    TARGET_MONTH: Optional[int] = _safe_int(os.getenv("TARGET_MONTH"))
    TARGET_YEAR: Optional[int] = _safe_int(os.getenv("TARGET_YEAR"))

    # --- Parametros de Conexao: Supabase ---
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    
    # --- Parametros de Conexao: TOTVS Protheus ---
    TOTVS_URL: str = os.getenv("TOTVS_URL", "")
    TOTVS_USER: str = os.getenv("TOTVS_USER", "")
    TOTVS_PASS: str = os.getenv("TOTVS_PASS", "")

    # --- Configuracoes de Rede e Performance ---
    # API_TIMEOUT: Tempo maximo de espera por resposta da API (padrao 45s)
    # POLLING_INTERVAL: Intervalo entre ciclos de busca (padrao 60s)
    TIMEOUT_REQUEST: int = _safe_int(os.getenv("API_TIMEOUT")) or 45
    POLLING_INTERVAL: int = _safe_int(os.getenv("POLLING_INTERVAL")) or 60

    def __post_init__(self):
        """
        Garante a existencia da estrutura fisica de diretorios 
        necessaria para o funcionamento dos logs e do banco de dados.
        """
        try:
            self.LOG_DIR.mkdir(parents=True, exist_ok=True)
            self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            # Uso de print aqui pois o logger pode ainda nao estar inicializado
            print(f"[ERRO CRITICO] Falha ao criar estrutura de diretorios: {e}")

# Instancia unica (Singleton) para ser importada em todo o projeto
CONFIG = AppConfig()