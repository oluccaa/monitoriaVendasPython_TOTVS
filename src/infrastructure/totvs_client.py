# src/infrastructure/totvs_client.py

import time
import httpx
import asyncio
from src.infrastructure.logging import logger
from src.config import CONFIG

class TOTVSClient:
    def __init__(self, base_url: str, username: str, password: str):
        """
        Cliente REST Assíncrono para integração com o Protheus da Acos Vital.
        Configurado com estratégias de retentativa para maior resiliência.
        """
        self.base_url = base_url
        self.auth = httpx.BasicAuth(username, password)
        # Timeout rigoroso de 45s (igual ao original)
        self.timeout = httpx.Timeout(45.0)
        
        # Configuração da estratégia de retry preservada
        self.max_retries = 3
        self.retry_status_codes = {429, 500, 502, 503, 504}

    async def _fetch_with_retry(self, client: httpx.AsyncClient, params: dict) -> dict:
        """
        Executa a requisição com política de retentativas automáticas e backoff exponencial.
        Substitui o HTTPAdapter/Retry do antigo requests.
        """
        for tentativa in range(self.max_retries + 1):
            try:
                response = await client.get(self.base_url, params=params)
                
                # Se o status code for um erro de servidor (ex: 503), tentamos novamente
                if response.status_code in self.retry_status_codes and tentativa < self.max_retries:
                    logger.warning(f"[TOTVS] Instabilidade (Status {response.status_code}). Retentativa {tentativa + 1}/{self.max_retries}...")
                    await asyncio.sleep(1 * (2 ** tentativa))  # Espera 1s, 2s, 4s...
                    continue
                
                # Levanta exceção para outros erros (ex: 401, 404) ou se esgotou as tentativas
                response.raise_for_status()
                return response.json()
                
            except httpx.RequestError as e:
                # Falhas de rede (timeout, conexão recusada, etc)
                if tentativa < self.max_retries:
                    logger.warning(f"[TOTVS] Falha de rede: {e}. Retentativa {tentativa + 1}/{self.max_retries}...")
                    await asyncio.sleep(1 * (2 ** tentativa))
                    continue
                raise  # Esgotou as tentativas, repassa o erro para o bloco principal

    async def fetch_sales_orders(self) -> list:
        """
        Consulta todos os pedidos de venda utilizando paginação para evitar timeout.
        Executa de forma assíncrona para não travar o event loop principal.
        """
        start_time = time.perf_counter()
        logger.info("[TOTVS] Iniciando busca assíncrona de pedidos na API REST.")
        
        todos_pedidos = []
        pagina_atual = 1
        tamanho_pagina = 100
        
        try:
            # O AsyncClient gerencia o pool de conexões (session) automaticamente
            async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                while True:
                    params = {
                        "page": pagina_atual,
                        "pageSize": tamanho_pagina
                    }
                    
                    # Chamada encapsulada com a lógica de retentativa
                    dados = await self._fetch_with_retry(client, params)
                    
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
            
        except httpx.HTTPError as e:
            # Mantém exatamente a mesma assinatura de tratamento de erro e log da versão anterior
            duracao = time.perf_counter() - start_time
            logger.error(f"[TOTVS] Falha na comunicacao apos {duracao:.4f}s. Detalhes: {str(e)}")
            return []

    def _filtrar_payload(self, raw_data: list) -> list:
        """
        Extrai e sanitiza apenas os campos necessarios para o banco de dados.
        Aplica o filtro de Mês e Ano definidos no .env para travar meses antigos.
        """
        pedidos_processados = []
        
        # 1. Prepara o prefixo de filtro de data (ex: "2026-03")
        filtro_data = None
        if CONFIG.TARGET_YEAR and CONFIG.TARGET_MONTH:
            # zfill(2) garante que o mês '3' vire '03', batendo com o padrão 'YYYY-MM-DD'
            mes_formatado = str(CONFIG.TARGET_MONTH).zfill(2)
            filtro_data = f"{CONFIG.TARGET_YEAR}-{mes_formatado}"
        
        for item in raw_data:
            # Validação basica de integridade do registro
            if not isinstance(item, dict) or not item.get("orderid"):
                continue
                
            data_emissao = str(item.get("issuedate", "")).strip()
            
            # 2. A TRAVA: Se o filtro estiver ativo e a data não bater, ignora a linha
            if filtro_data and not data_emissao.startswith(filtro_data):
                continue
                
            pedidos_processados.append({
                "orderid": str(item.get("orderid")).strip(),
                "issuedate": data_emissao,
                "sellerid": str(item.get("sellerid")).strip(),
                "amount": float(item.get("amount", 0.0)),
                "sellername": str(item.get("sellername", "DESCONHECIDO")).strip().upper(),
                "customername": str(item.get("customername", "DESCONHECIDO")).strip().upper()
            })
            
        # Loga quantos registros sobraram apos o filtro
        if filtro_data:
            logger.info(f"[TOTVS] Filtro aplicado ({filtro_data}). Retornando {len(pedidos_processados)} pedidos para sincronizacao.")
            
        return pedidos_processados