# src/infrastructure/__init__.py

from .database import DatabaseRepository
# REMOVIDO: from .http_client import WebhookClient
# ADICIONADO:
from .supabase_client import SupabaseRepository
from .logging import logger, setup_logger