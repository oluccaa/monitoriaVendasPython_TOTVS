import time
import requests
from requests.auth import HTTPBasicAuth
from src.infrastructure.logging import logger

class TOTVSClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        # Configuracao de timeout e retentativas omitida para brevidade

    def buscar_novos_pedidos(self, ultima_verificacao: str) -> list:
        start_time = time.perf_counter()
        logger.info(f"[TOTVS] Iniciando busca de pedidos a partir de {ultima_verificacao}")
        
        try:
            # Aqui voce passaria parametros para filtrar pela data/hora
            response = self.session.get(self.base_url, auth=self.auth, timeout=30)
            response.raise_for_status()
            dados = response.json()
            
            duracao = time.perf_counter() - start_time
            logger.info(f"[TOTVS] Busca concluida em {duracao:.4f}s. Encontrados {len(dados)} registros.")
            
            return self._filtrar_payload(dados)
            
        except Exception as e:
            duracao = time.perf_counter() - start_time
            logger.error(f"[TOTVS] Falha na comunicacao apos {duracao:.4f}s. Erro: {str(e)}")
            return []

    def _filtrar_payload(self, raw_data: list) -> list:
        pedidos_processados = []
        for item in raw_data:
            pedidos_processados.append({
                "orderid": item.get("orderid"),
                "issuedate": item.get("issuedate"),
                "sellerid": item.get("sellerid"),
                "amount": item.get("amount"),
                "sellername": item.get("sellername"),
                "customername": item.get("customername")
            })
        return pedidos_processados