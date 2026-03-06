# src/infrastructure/http_client.py

# 1. Imports da Biblioteca Padrão
import time
import requests
from typing import List, Dict, Any

# 2. Imports de Terceiros (Requests & Urllib3)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 3. Imports da Aplicação
from src.config import CONFIG
from src.infrastructure.logging import logger

# ==============================================================================
# INFRAESTRUTURA: CLIENTE HTTP (WEBHOOK)
# ==============================================================================

class WebhookClient:
    def __init__(self, url: str, token: str):
        """
        Cliente HTTP resiliente para comunicação com o n8n.
        
        Args:
            url: Endpoint do webhook.
            token: Token de segurança para validação no n8n.
        """
        self.url = url
        self.token = token
        self.session = self._make_session()

    def _make_session(self) -> requests.Session:
        """
        Cria uma sessão HTTP com estratégia de retentativa (Retry).
        Se a rede falhar, ele tenta 3 vezes antes de desistir.
        """
        session = requests.Session()
        
        # Estratégia: 3 tentativas totais
        # Backoff factor 1: espera 1s, depois 2s, depois 4s...
        # Status forcelist: tenta de novo se receber erro 500, 502, 503, 504 (Server Error)
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session

    def send_deltas(self, vendas: List[Dict[str, Any]]) -> bool:
        """
        Envia um lote de vendas para o n8n.
        
        Returns:
            True se o envio foi aceito (200 OK), False caso contrário.
        """
        if not vendas:
            return True
            
        try:
            payload = {
                "token": self.token, 
                "timestamp_origem": time.time(),
                "vendas": vendas
            }
            
            # Timeout configurado no config.py para evitar travamentos
            response = self.session.post(
                self.url, 
                json=payload, 
                timeout=CONFIG.TIMEOUT_REQUEST,
                headers={"Content-Type": "application/json"}
            )
            
            # --- FREIO ESTRATÉGICO ---
            # Aguarda 2 segundos após CADA envio bem-sucedido.
            # Motivo: Evita que o n8n e a TV recebam 30 atualizações no mesmo segundo
            # e causem "flicker" (piscadas) ou sobrecarga no banco da TV.
            if response.status_code == 200:
                time.sleep(2) 
                return True
            # -------------------------
            
            logger.error(f"❌ [API] n8n recusou os dados (Status {response.status_code}): {response.text[:200]}")
            return False

        except requests.exceptions.RequestException as re:
            logger.error(f"📡 [API] Erro de rede ao conectar no n8n: {re}")
            return False
        except Exception as e:
            logger.error(f"💥 [API] Erro inesperado no envio: {e}")
            return False