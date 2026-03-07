import time
from typing import TYPE_CHECKING
from src.infrastructure.logging import logger

if TYPE_CHECKING:
    from src.application.sync import SyncService
    from src.infrastructure.totvs_client import TOTVSClient

class TOTVSPoller:
    def __init__(self, totvs_client: 'TOTVSClient', sync_service: 'SyncService', interval_seconds: int = 60):
        self.totvs_client = totvs_client
        self.sync_service = sync_service
        self.interval_seconds = interval_seconds
        self._is_running = False

    def start(self):
        self._is_running = True
        logger.info(f"[SISTEMA] Modo Polling ativado. Consultando TOTVS a cada {self.interval_seconds} segundos.")

        try:
            while self._is_running:
                self._run_cycle()
                self._sleep_interruptible()
                
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.critical(f"[SISTEMA] Erro catastrofico no loop de polling: {str(e)}", exc_info=True)
            self.stop()

    def _sleep_interruptible(self):
        logger.info(f"[POLLER] Aguardando {self.interval_seconds}s para a proxima verificacao...")
        for _ in range(self.interval_seconds):
            if not self._is_running:
                break
            time.sleep(1)

    def stop(self):
        if self._is_running:
            logger.info("[SISTEMA] Sinal de desligamento recebido. Encerrando Poller graciosamente...")
            self._is_running = False

    def _run_cycle(self):
        start_time = time.perf_counter()
        logger.info("[POLLER] Iniciando ciclo de verificacao de novos pedidos.")
        
        try:
            pedidos_totvs = self.totvs_client.fetch_sales_orders()
            
            if pedidos_totvs:
                logger.info(f"[POLLER] {len(pedidos_totvs)} pedidos carregados. Iniciando SyncService.")
                self.sync_service.process_totvs_payload(pedidos_totvs)
            else:
                logger.info("[POLLER] Nenhum pedido retornado ou lista vazia. Ignorando sincronizacao.")
                
        except Exception as e:
            logger.error(f"[POLLER] Falha isolada durante o ciclo de verificacao: {str(e)}", exc_info=True)
            
        finally:
            duracao = time.perf_counter() - start_time
            logger.info(f"[POLLER] Ciclo finalizado em {duracao:.2f}s.")