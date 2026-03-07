# src/application/poller.py

# 1. Imports da Biblioteca Padrao
import time
from typing import TYPE_CHECKING

# 2. Imports da Infraestrutura
from src.infrastructure.logging import logger

# 3. Imports para Type Hinting (Evita importacao circular em tempo de execucao)
if TYPE_CHECKING:
    from src.application.sync import SyncService
    from src.infrastructure.totvs_client import TOTVSClient

# ==============================================================================
# POLLER: ORQUESTRADOR DE SINCRONIZACAO CONTINUA (CAMADA DE APLICACAO)
# ==============================================================================

class TOTVSPoller:
    def __init__(self, totvs_client: 'TOTVSClient', sync_service: 'SyncService', interval_seconds: int = 60):
        """
        Orquestra a consulta ativa a API do TOTVS em intervalos regulares.
        
        Args:
            totvs_client: Cliente HTTP para buscar dados no TOTVS.
            sync_service: Servico responsavel pelo Delta e envio ao banco.
            interval_seconds: Tempo de espera entre cada consulta.
        """
        self.totvs_client = totvs_client
        self.sync_service = sync_service
        self.interval_seconds = interval_seconds
        self._is_running = False

    def start(self):
        """Inicia o loop principal do sistema (Modo Polling)."""
        self._is_running = True
        logger.info(f"[SISTEMA] Modo Polling ativado. Consultando TOTVS a cada {self.interval_seconds} segundos.")

        try:
            while self._is_running:
                self._run_cycle()
                
                # Pausa controlada para permitir interrupcao rapida
                self._sleep_interruptible()
                
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.critical(f"[SISTEMA] Erro catastrofico no loop de polling: {str(e)}", exc_info=True)
            self.stop()

    def _sleep_interruptible(self):
        """
        Dorme em fragmentos de 1 segundo para permitir encerramento rapido (Graceful Shutdown).
        Evita o bloqueio da thread principal caso o usuario interrompa o processo.
        """
        logger.info(f"[POLLER] Aguardando {self.interval_seconds}s para a proxima verificacao...")
        for _ in range(self.interval_seconds):
            if not self._is_running:
                break
            time.sleep(1)

    def stop(self):
        """Encerra o loop de monitoramento de forma graciosa."""
        if self._is_running:
            logger.info("[SISTEMA] Sinal de desligamento recebido. Encerrando Poller graciosamente...")
            self._is_running = False

    def _run_cycle(self):
        """Executa um ciclo individual de busca no TOTVS e envio ao banco."""
        start_time = time.perf_counter()
        logger.info("[POLLER] Iniciando ciclo de verificacao de novos pedidos.")
        
        try:
            # 1. Busca os dados brutos da API TOTVS (via infraestrutura)
            pedidos_totvs = self.totvs_client.fetch_sales_orders()
            
            # 2. Encaminha para o processamento de regras e banco de dados
            if pedidos_totvs:
                logger.info(f"[POLLER] {len(pedidos_totvs)} pedidos carregados. Iniciando SyncService.")
                self.sync_service.process_totvs_payload(pedidos_totvs)
            else:
                logger.info("[POLLER] Nenhum pedido retornado ou lista vazia. Ignorando sincronizacao.")
                
        except Exception as e:
            # Captura erros isolados para garantir que o loop continue rodando em proximos ciclos
            logger.error(f"[POLLER] Falha isolada durante o ciclo de verificacao: {str(e)}", exc_info=True)
            
        finally:
            duracao = time.perf_counter() - start_time
            logger.info(f"[POLLER] Ciclo finalizado em {duracao:.2f}s.")