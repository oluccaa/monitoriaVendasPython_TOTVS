import time
import requests
from requests.auth import HTTPBasicAuth
from src.infrastructure.logging import logger

class TOTVSClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()

    def fetch_sales_orders(self) -> list:
        start_time = time.perf_counter()
        logger.info("[TOTVS] Iniciando busca de pedidos na API REST.")
        
        todos_pedidos = []
        pagina_atual = 1
        tamanho_pagina = 100
        
        try:
            while True:
                params = {
                    "page": pagina_atual,
                    "pageSize": tamanho_pagina
                }
                
                response = self.session.get(
                    self.base_url, 
                    auth=self.auth, 
                    params=params,
                    timeout=45
                )
                response.raise_for_status()
                
                dados = response.json()
                lista_pedidos = dados if isinstance(dados, list) else dados.get('items', [])
                
                if not lista_pedidos:
                    break
                    
                todos_pedidos.extend(lista_pedidos)
                logger.info(f"[TOTVS] Pagina {pagina_atual} processada. {len(lista_pedidos)} registros obtidos.")
                
                if len(lista_pedidos) < tamanho_pagina:
                    break
                    
                pagina_atual += 1
                
            duracao = time.perf_counter() - start_time
            logger.info(f"[TOTVS] Download concluido em {duracao:.4f}s. Total bruto: {len(todos_pedidos)}.")
            
            return self._filtrar_payload(todos_pedidos)
            
        except requests.exceptions.RequestException as e:
            duracao = time.perf_counter() - start_time
            logger.error(f"[TOTVS] Falha na comunicacao apos {duracao:.4f}s. Erro: {str(e)}")
            return []

    def _filtrar_payload(self, raw_data: list) -> list:
        pedidos_processados = []
        for item in raw_data:
            if not isinstance(item, dict) or not item.get("orderid"):
                continue
                
            pedidos_processados.append({
                "orderid": item.get("orderid"),
                "issuedate": item.get("issuedate"),
                "sellerid": item.get("sellerid"),
                "amount": float(item.get("amount", 0.0)),
                "sellername": item.get("sellername", "DESCONHECIDO"),
                "customername": item.get("customername", "DESCONHECIDO")
            })
        return pedidos_processados