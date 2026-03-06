import sqlite3
import os

def deletar_cache_personalizado(vendedor=None, mes=None, ano=None):
    # Caminho conforme sua estrutura de pastas
    caminho_bd = os.path.join('data', 'sentinel_cache.db')
    
    if not os.path.exists(caminho_bd):
        print(f"Erro: Arquivo não encontrado em {caminho_bd}")
        return

    try:
        conn = sqlite3.connect(caminho_bd)
        cursor = conn.cursor()

        # Base da Query
        sql = "DELETE FROM linhas_processadas WHERE 1=1"
        parametros = []

        # Adiciona filtros dinamicamente conforme o que você preencher
        if vendedor:
            sql += " AND vendedor = ?"
            parametros.append(vendedor)
        
        if mes:
            sql += " AND strftime('%m', data_envio) = ?"
            parametros.append(str(mes).zfill(2)) # Garante formato '02'
            
        if ano:
            sql += " AND strftime('%Y', data_envio) = ?"
            parametros.append(str(ano))

        # Execução
        cursor.execute(sql, parametros)
        conn.commit()
        
        print(f"--- Relatório de Exclusão ---")
        print(f"Filtros aplicados: Vendedor={vendedor}, Mês={mes}, Ano={ano}")
        print(f"Registros removidos: {cursor.rowcount}")
        print("-" * 30)

    except sqlite3.Error as e:
        print(f"Erro no SQLite: {e}")
    finally:
        if conn:
            conn.close()

# --- EXEMPLOS DE USO ---

# 1. Apagar apenas de um vendedor específico em um mês/ano
# deletar_cache_personalizado(vendedor="EBER VIEIRA", mes="02", ano="2026")

# 2. Apagar tudo de um vendedor (independente da data)
# deletar_cache_personalizado(vendedor="PRISCILA YUMI")

# 3. Apagar o mês inteiro de todos os vendedores (como era antes)
# deletar_cache_personalizado(mes="2", ano="2026")