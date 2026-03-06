# monitor_vendas.py

# 1. Imports da Biblioteca Padrão
import sys
import time
import os
import re 
from pathlib import Path
from watchdog.observers import Observer

# ==============================================================================
# IMPORTAÇÕES DA NOVA ARQUITETURA (DDD)
# ==============================================================================
try:
    # 1. Configuração Global
    from src.config import CONFIG
    
    # 2. Infraestrutura (Ferramentas Técnicas)
    from src.infrastructure import (
        logger, 
        DatabaseRepository, 
        SupabaseRepository 
    )
    
    # 3. Aplicação (Orquestração)
    from src.application import (
        SyncService, 
        SentinelEventHandler
    )
    
    # 4. Domínio (Regras de Negócio e Auditoria)
    from src.domain.auditor import AUDITOR

except ImportError as e:
    print(f"❌ ERRO CRÍTICO DE IMPORTAÇÃO: {e}")
    print("Verifique se a pasta 'src' existe e tem os arquivos __init__.py configurados.")
    print("Dica: Execute este script da raiz do projeto (onde está o arquivo .env).")
    sys.exit(1)

# ==============================================================================
# MAIN & BOOTSTRAP
# ==============================================================================

def run_sentinel():
    """Inicialização com Recovery Profissional e Resiliência de Rede."""
    
    logger.info("===================================================")
    logger.info(f"   🚀 {CONFIG.APP_NAME.upper()} - INICIANDO SISTEMA   ")
    logger.info("===================================================")

    # 1. Inicialização de Infraestrutura (Injeção de Dependência)
    repo = DatabaseRepository(CONFIG.DB_PATH)
    
    # --- CONEXÃO SUPABASE ---
    try:
        client = SupabaseRepository()
        logger.info("☁️  [SUPABASE] Cliente conectado com sucesso.")

        # --- CACHE DE VENDEDORES ---
        logger.info("🔍 [CACHE] Carregando lista de vendedores ativos...")
        vendedores_cache = client.get_vendedores_ativos()
        
        if not vendedores_cache:
            logger.critical("🚨 [FATAL] Nenhum vendedor ativo encontrado no banco. Abortando.")
            return
        
        logger.info(f"👥 [CACHE] {len(vendedores_cache)} vendedores autorizados carregados.")
    except Exception as e:
        logger.critical(f"🚨 [FATAL] Erro ao conectar no Supabase: {e}")
        return

    # 2. Inicialização dos Serviços
    sync_service = SyncService(repo, client)
    event_handler = SentinelEventHandler(sync_service, vendedores_cache)

    # ==========================================================================
    # 3. [WARM-UP] PRÉ-CARREGAMENTO DA AUDITORIA
    # ==========================================================================
    # Antes de verificar os arquivos, garantimos que a base da Omie está atualizada.
    # Isso evita lentidão na primeira leitura.
    if CONFIG.TARGET_MONTH and CONFIG.TARGET_YEAR:
        logger.info(f"⏳ [BOOT] Iniciando pré-carregamento Omie para {CONFIG.TARGET_MONTH:02d}/{CONFIG.TARGET_YEAR}...")
        try:
            AUDITOR.prepare_context(CONFIG.TARGET_MONTH, CONFIG.TARGET_YEAR)
            logger.info("✅ [BOOT] Base Omie carregada e pronta para uso!")
        except Exception as e:
            logger.warning(f"⚠️ [BOOT] Falha no pré-carregamento Omie (O sistema tentará novamente sob demanda): {e}")
    else:
        logger.info("ℹ️ [BOOT] TARGET_MONTH não definido. O download da Omie será feito sob demanda.")

    # ==========================================================================
    # 4. VERIFICAÇÃO DE REDE E RECOVERY
    # ==========================================================================
    logger.info("📡 [REDE] Verificando disponibilidade das unidades de rede...")
    pastas_validas = []
    
    # CONFIG.WATCH_PATH pode ser uma string única ou lista, ajustamos para garantir iteração
    paths_to_check = CONFIG.PASTAS_MONITORADAS if hasattr(CONFIG, 'PASTAS_MONITORADAS') else [CONFIG.WATCH_PATH]

    for folder in paths_to_check:
        path_obj = Path(folder)
        try:
            if path_obj.exists():
                # Tenta listar para garantir permissão de leitura
                os.listdir(str(path_obj)) 
                pastas_validas.append(path_obj)
                logger.info(f"✅ [REDE] Unidade acessível: {folder}")
            else:
                logger.warning(f"⚠️ [REDE] Unidade offline ou não mapeada: {folder}")
        except Exception as e:
            logger.error(f"❌ [REDE] Falha crítica ao acessar {folder}: {e}")

    if not pastas_validas:
        logger.critical("🚨 [FATAL] Nenhuma unidade de rede disponível. O Sentinel não pode iniciar.")
        return

    # 5. RECOVERY MODE (Sincronização Gradual)
    # Configuração da Whitelist para varredura inicial
    mapa_meses = {
        1: r'01_JANEIRO', 2: r'02_FEVEREIRO', 3: r'03_MAR[CÇ]O',
        4: r'04_ABRIL', 5: r'05_MAIO', 6: r'06_JUNHO',
        7: r'07_JULHO', 8: r'08_AGOSTO', 9: r'09_SETEMBRO',
        10: r'10_OUTUBRO', 11: r'11_NOVEMBRO', 12: r'12_DEZEMBRO'
    }

    if CONFIG.TARGET_MONTH and CONFIG.TARGET_MONTH in mapa_meses:
        padrao_mes = mapa_meses[CONFIG.TARGET_MONTH]
        logger.info(f"🎯 [RECOVERY] Modo Focado: Varrendo APENAS arquivos de '{padrao_mes}'")
    else:
        padrao_mes = "|".join(mapa_meses.values())
        logger.info("👀 [RECOVERY] Modo Global: Varrendo todos os meses fiscais.")

    whitelist_pattern = re.compile(
        rf'^({padrao_mes})_RELATORIO-DE-VENDAS\.xlsx?$',
        re.IGNORECASE | re.VERBOSE
    )

    logger.info("🔄 [RECOVERY] Iniciando Varredura Gradual...")
    arquivos_para_processar = []

    for folder in pastas_validas:
        for root, _, files in os.walk(str(folder)):
            for file in files:
                # 1. Ignora temporários e extensões erradas
                if file.startswith('~$') or not file.lower().endswith(('.xlsx', '.xls')):
                    continue

                # 2. Aplica Whitelist
                if not whitelist_pattern.match(file):
                    continue

                arquivos_para_processar.append(os.path.join(root, file))

    if arquivos_para_processar:
        logger.info(f"🔎 [RECOVERY] {len(arquivos_para_processar)} arquivos OFICIAIS encontrados.")
        for i, arquivo in enumerate(arquivos_para_processar, 1):
            nome_simples = os.path.basename(arquivo)
            logger.info(f"📦 [{i}/{len(arquivos_para_processar)}] Sincronizando: {nome_simples}")
            event_handler._handle_file(arquivo)
            time.sleep(1) # Pausa para não sobrecarregar
        logger.info("✅ [RECOVERY] Sincronização concluída.")
    else:
        logger.info("ℹ️ [RECOVERY] Nenhum arquivo compatível encontrado.")

    # 6. Monitoramento em Tempo Real (Observer)
    observer = Observer()
    for folder in pastas_validas:
        observer.schedule(event_handler, str(folder), recursive=True)
    
    try:
        observer.start()
        logger.info("🛡️  [SISTEMA] Modo Sentinela Ativado. Monitoramento iniciado.")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("🛑 [SISTEMA] Desligamento solicitado pelo usuário.")
        observer.stop()
    except Exception as e:
        logger.critical(f"💥 [SISTEMA] Erro catastrófico no loop principal: {e}")
        observer.stop()
    finally:
        observer.join()
        logger.info("👋 [SISTEMA] Sentinel encerrado.")

if __name__ == "__main__":
    if os.name == 'nt':
        os.system(f"title {CONFIG.APP_NAME}")
    
    if "--boot" in sys.argv:
        logger.info("⏳ Aguardando estabilização do sistema (30s)...")
        time.sleep(30)
        
    run_sentinel()