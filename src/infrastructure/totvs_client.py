# src/infrastructure/totvs_client.py

import time
import httpx
import asyncio
import json
from pathlib import Path
from src.infrastructure.logging import logger
from src.config import CONFIG

class TOTVSClient:
    def __init__(self, base_url: str, username: str, password: str):
        """
        Cliente REST Assincrono para integracao com o Protheus da Acos Vital.
        """
        self.base_url = base_url
        self.auth = httpx.BasicAuth(username, password)
        self.timeout = httpx.Timeout(45.0)
        
        self.max_retries = 3
        self.retry_status_codes = {429, 500, 502, 503, 504}
        
        # Caminho para o arquivo JSON de pedidos ignorados
        self.ignore_file_path = CONFIG.BASE_DIR / "data" / "ignorar_pedidos.json"

    def _carregar_pedidos_ignorados(self) -> set:
        """Lê o arquivo JSON e retorna um 'set' (conjunto) para busca ultra-rápida."""
        if self.ignore_file_path.exists():
            try:
                with open(self.ignore_file_path, 'r', encoding='utf-8') as f:
                    dados = json.load(f)
                    if isinstance(dados, list):
                        # Converte tudo para string e remove espaços para evitar erros de digitação
                        return set(str(pedido).strip() for pedido in dados)
            except Exception as e:
                logger.error(f"[TOTVS] Erro ao ler {self.ignore_file_path.name}: {e}")
        return set() # Retorna vazio se o arquivo não existir ou der erro

    async def _fetch_with_retry(self, client: httpx.AsyncClient, params: dict) -> dict:
        # ... (MANTENHA ESTE MÉTODO EXATAMENTE COMO ESTAVA) ...
        for tentativa in range(self.max_retries + 1):
            try:
                response = await client.get(self.base_url, params=params)
                
                if response.status_code in self.retry_status_codes and tentativa < self.max_retries:
                    logger.warning(f"[TOTVS] Instabilidade (Status {response.status_code}). Retentativa {tentativa + 1}/{self.max_retries}...")
                    await asyncio.sleep(1 * (2 ** tentativa))
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except httpx.RequestError as e:
                if tentativa < self.max_retries:
                    logger.warning(f"[TOTVS] Falha de rede: {e}. Retentativa {tentativa + 1}/{self.max_retries}...")
                    await asyncio.sleep(1 * (2 ** tentativa))
                    continue
                raise

    async def fetch_sales_orders(self) -> list:
        start_time = time.perf_counter()
        logger.info("[TOTVS] Iniciando busca assincrona de pedidos na API REST.")
        
        todos_pedidos = []
        pagina_atual = 1
        tamanho_pagina = 100
        
        # ---> CARREGA A LISTA DE BLOQUEADOS AQUI <---
        pedidos_ignorados = self._carregar_pedidos_ignorados()
        if pedidos_ignorados:
            logger.info(f"[TOTVS] Lista de bloqueio ativada: {len(pedidos_ignorados)} pedidos na blacklist.")
        
        try:
            async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                while True:
                    params = {"page": pagina_atual, "pageSize": tamanho_pagina}
                    dados = await self._fetch_with_retry(client, params)
                    
                    lista_pedidos = dados if isinstance(dados, list) else dados.get('items', [])
                    if not lista_pedidos:
                        break
                        
                    todos_pedidos.extend(lista_pedidos)
                    logger.info(f"[TOTVS] Pagina {pagina_atual} processada. Registros: {len(lista_pedidos)}.")
                    
                    if len(lista_pedidos) < tamanho_pagina:
                        break
                    pagina_atual += 1
                    
            duracao = time.perf_counter() - start_time
            logger.info(f"[TOTVS] Operacao concluida em {duracao:.4f}s. Total bruto: {len(todos_pedidos)} registros.")
            
            # ---> PASSA A LISTA DE BLOQUEADOS PARA O FILTRO <---
            return self._filtrar_payload(todos_pedidos, pedidos_ignorados)
            
        except httpx.HTTPError as e:
            duracao = time.perf_counter() - start_time
            logger.error(f"[TOTVS] Falha na comunicacao apos {duracao:.4f}s. Detalhes: {str(e)}")
            return []

    def _filtrar_payload(self, raw_data: list, pedidos_ignorados: set) -> list:
        pedidos_processados = []
        
        filtro_data = None
        if CONFIG.TARGET_YEAR and CONFIG.TARGET_MONTH:
            mes_formatado = str(CONFIG.TARGET_MONTH).zfill(2)
            filtro_data = f"{CONFIG.TARGET_YEAR}-{mes_formatado}"
            
        bloqueados_count = 0 # Contador para sabermos quantos foram barrados
        
        for item in raw_data:
            if not isinstance(item, dict) or not item.get("orderid"):
                continue
            
            orderid = str(item.get("orderid")).strip()
            data_emissao = str(item.get("issuedate", "")).strip()
            
            # ---> A NOVA TRAVA: Verifica se o pedido esta no JSON <---
            if orderid in pedidos_ignorados:
                bloqueados_count += 1
                continue # Pula este pedido, ele não vai subir!
            
            # A trava antiga de Mês/Ano
            if filtro_data and not data_emissao.startswith(filtro_data):
                continue
                
            pedidos_processados.append({
                "orderid": orderid,
                "issuedate": data_emissao,
                "sellerid": str(item.get("sellerid")).strip(),
                "amount": float(item.get("amount", 0.0)),
                "sellername": str(item.get("sellername", "DESCONHECIDO")).strip().upper(),
                "customername": str(item.get("customername", "DESCONHECIDO")).strip().upper()
            })
            
        if filtro_data:
            logger.info(f"[TOTVS] Filtro aplicado ({filtro_data}).")
        if bloqueados_count > 0:
            logger.info(f"[TOTVS] {bloqueados_count} pedidos foram ignorados pela Blacklist (JSON).")
            
        return pedidos_processados