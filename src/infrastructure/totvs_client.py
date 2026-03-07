# src/infrastructure/totvs_client.py

import time
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.infrastructure.logging import logger

class TOTVSClient:
    def __init__(self, base_url: str, username: str, password: str):
        """
        Cliente REST para integração com o Protheus da Acos Vital.
        Configurado com estratégias de retentativa para maior resiliência.
        """
        self.base_url = base_url
        self.auth = HTTPBasicAuth(username, password)
        self.session = self._setup_session()

    def _setup_session(self) -> requests.Session:
        """Configura a sessão HTTP com política de retentativas automáticas."""
        session = requests.Session()
        
        # Estratégia: Tenta 3 vezes em caso de erros de servidor ou limites de taxa
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session

    def fetch_sales_orders(self) -> list:
        """
        Consulta todos os pedidos de venda utilizando paginação para evitar timeout.
        Retorna uma lista de dicionários mapeados com os campos essenciais.
        """
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
                
                # Execução da requisição com timeout rigoroso
                response = self.session.get(
                    self.base_url, 
                    auth=self.auth, 
                    params=params,
                    timeout=45
                )
                response.raise_for_status()
                
                dados = response.json()
                
                # O TOTVS pode retornar a lista direta ou encapsulada em 'items'
                lista_pedidos = dados if isinstance(dados, list) else dados.get('items', [])
                
                if not lista_pedidos:
                    break
                    
                todos_pedidos.extend(lista_pedidos)
                logger.info(f"[TOTVS] Pagina {pagina_atual} processada. Registros: {len(lista_pedidos)}.")
                
                # Se a página veio incompleta, chegamos ao fim dos dados
                if len(lista_pedidos) < tamanho_pagina:
                    break
                    
                pagina_atual += 1
                
            duracao = time.perf_counter() - start_time
            logger.info(f"[TOTVS] Operacao concluida em {duracao:.4f}s. Total bruto: {len(todos_pedidos)} registros.")
            
            return self._filtrar_payload(todos_pedidos)
            
        except requests.exceptions.RequestException as e:
            duracao = time.perf_counter() - start_time
            logger.error(f"[TOTVS] Falha na comunicacao apos {duracao:.4f}s. Detalhes: {str(e)}")
            return []

    def _filtrar_payload(self, raw_data: list) -> list:
        """
        Extrai e sanitiza apenas os campos necessários para o banco de dados.
        Campos: orderid, issuedate, sellerid, amount, sellername, customername.
        """
        pedidos_processados = []
        
        for item in raw_data:
            # Validação básica de integridade do registro
            if not isinstance(item, dict) or not item.get("orderid"):
                continue
                
            pedidos_processados.append({
                "orderid": str(item.get("orderid")).strip(),
                "issuedate": str(item.get("issuedate")).strip(),
                "sellerid": str(item.get("sellerid")).strip(),
                "amount": float(item.get("amount", 0.0)),
                "sellername": str(item.get("sellername", "DESCONHECIDO")).strip().upper(),
                "customername": str(item.get("customername", "DESCONHECIDO")).strip().upper()
            })
            
        return pedidos_processados