# src/infrastructure/database.py

# 1. Imports da Biblioteca Padrão
import sqlite3
from pathlib import Path
from typing import List, Dict, Tuple

# 2. Imports da Aplicação
from src.infrastructure.logging import logger

# ==============================================================================
# INFRAESTRUTURA: REPOSITÓRIO (BANCO DE DADOS)
# ==============================================================================

class DatabaseRepository:
    def __init__(self, db_path: Path):
        """
        Inicializa o repositório SQLite.
        
        Args:
            db_path: Caminho completo para o arquivo .db.
        """
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Configura uma conexão otimizada para alta concorrência."""
        # Timeout de 30s evita erros imediatos de 'Database Locked'
        conn = sqlite3.connect(self.db_path, timeout=30)
        
        # Modo WAL (Write-Ahead Logging) permite leituras e escritas simultâneas
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA busy_timeout=5000') # Espera 5s antes de falhar se estiver ocupado
        
        return conn

    def _init_db(self):
        """Cria as tabelas necessárias se não existirem."""
        try:
            # Cria o diretório pai se não existir (evita erro de FileNotFoundError)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            with self._get_connection() as conn:
                # ATUALIZAÇÃO: Adicionadas colunas mes_ref e ano_ref
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS linhas_processadas (
                        id_linha TEXT PRIMARY KEY,
                        hash_linha TEXT,
                        vendedor TEXT,
                        mes_ref TEXT,
                        ano_ref INTEGER,
                        data_envio TIMESTAMP
                    )
                ''')
                
                # Novo índice composto para busca rápida por período (Evita apagar mês errado)
                conn.execute('CREATE INDEX IF NOT EXISTS idx_periodo ON linhas_processadas(vendedor, mes_ref, ano_ref)')
                
            logger.info(f"✅ [DB] Banco de dados verificado e otimizado: {self.db_path}")
            
        except sqlite3.Error as e:
            logger.critical(f"💥 [DB] Erro fatal ao iniciar banco: {e}")
            raise RuntimeError(f"Falha crítica no banco de dados: {e}")

    def get_cache_by_periodo(self, vendedor: str, mes: str, ano: int) -> Dict[str, str]:
        """
        Retorna snapshot APENAS do mês/ano sendo processado.
        ISSO É A SEGURANÇA QUE IMPEDE APAGAR DADOS DE OUTROS MESES.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''SELECT id_linha, hash_linha FROM linhas_processadas 
                    WHERE vendedor = ? AND mes_ref = ? AND ano_ref = ?''', 
                    (vendedor, str(mes), ano)
                )
                return {row[0]: row[1] for row in cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"❌ [DB] Erro leitura cache ({vendedor} {mes}/{ano}): {e}")
            return {}

    def get_cache_by_vendedor(self, vendedor: str) -> Dict[str, str]:
        """
        Retorna todo o histórico do vendedor (Método legado/genérico).
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT id_linha, hash_linha FROM linhas_processadas WHERE vendedor = ?', 
                    (vendedor,)
                )
                return {row[0]: row[1] for row in cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"❌ [DB] Erro ao ler cache para {vendedor}: {e}")
            return {}

    def update_batch(self, to_delete: List[Tuple], to_upsert: List[Tuple]):
        """
        Executa atualizações em lote (Batch) de forma atômica.
        """
        if not to_delete and not to_upsert:
            return

        try:
            with self._get_connection() as conn:
                if to_delete:
                    conn.executemany(
                        'DELETE FROM linhas_processadas WHERE id_linha = ?', 
                        to_delete
                    )
                
                if to_upsert:
                    # ATUALIZAÇÃO: Inserção agora inclui mes_ref e ano_ref
                    conn.executemany(
                        '''INSERT OR REPLACE INTO linhas_processadas 
                        (id_linha, hash_linha, vendedor, mes_ref, ano_ref, data_envio) 
                        VALUES (?, ?, ?, ?, ?, ?)''', 
                        to_upsert
                    )
                    
        except sqlite3.Error as e:
            logger.error(f"💥 [DB] Falha crítica no commit em lote: {e}")