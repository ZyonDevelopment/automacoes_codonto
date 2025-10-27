import sys
import os
import time
from functions import get_downloads_dir, log

# garante path raiz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functions import (
    iniciar_chrome,
    interagir_elementos,
    realizar_login_codonto,
    snapshot_downloads,
    aguardar_novo_download,
    fechar_navegador_assincrono,
    log,
)
from etl.etl_manager import rodar_etl_generico

# =========================================================
# ========== CONFIGURAÇÕES GERAIS ==========================
# =========================================================
PROJETO = "api-para-sheets-433311"
DATASET = "Dados_OdontoClean"
TABELA = "A_Receber"

# diretórios no servidor
# Caminho atual (onde está este arquivo)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Caminho da raiz do projeto (sobe 2 níveis: automations -> automacoes_codonto -> BOTS 5.0)
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
# Caminho da pasta de downloads dentro da raiz
DIRETORIO_DOWNLOADS = os.path.join(ROOT_DIR, "automacoes_codonto", "downloads")
# Garante que a pasta existe
os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)
CREDENCIAIS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "GBOQ.json")
)

print(f"[DEBUG] Pasta de downloads configurada: {DIRETORIO_DOWNLOADS}")

# nome esperado no arquivo
NOME_PADRAO_ARQUIVO = "ControleODONTO - Títulos a Receber"

# =========================================================
# ========== CONFIGURAÇÃO DE ETL (NOVO FORMATO) ===========
# =========================================================
ETL_CONFIG = {
    # parâmetros de leitura
    "skip_top": 2,
    "skip_bottom": 2,

    # função ETL específica
    "etl_especifico": "",
    "use_etl_pos": "",

    # parâmetros de upload BigQuery
    "tabela": TABELA,
    "coluna_validacao": "valor_devido",        # usada na soma de validação
    "periodo_coluna": "vencimento",                    # define o recorte de período
    "chaves_particao": [], # campos usados no DELETE

    # caminho de credenciais (opcional)
    "credenciais_path": CREDENCIAIS_PATH,
}

# =========================================================
# ========== FUNÇÃO PRINCIPAL DA AUTOMAÇÃO ================
# =========================================================
def executar_a_receber(usuario, senha, data_inicio, data_fim, zoom=0.8, pasta_download=None):
    """
    Executa a automação de 'Valores a Receber':
    - Faz login no Codonto.
    - Aplica filtros de data.
    - Baixa o relatório Excel.
    - Executa o ETL e faz upload ao BigQuery.
    """
    print("\n=== INICIANDO AUTOMAÇÃO: VALORES A RECEBER ===")

    if not pasta_download:
        # fallback seguro (mas ideal é vir do manager)
        pasta_download = get_downloads_dir()
        log("⚠️ pasta_download não informado — usando padrão.", "WARN")
        
    # 1) Iniciar navegador
    driver = iniciar_chrome(
        url_inicial="https://codonto.aplicativo.net/",
        zoom=zoom,
        pasta_download=pasta_download,
    )

    # 2) Login
    try:
        realizar_login_codonto(driver, usuario, senha)
        log("✅ Login realizado com sucesso", "OK")
    except Exception as e:
        log(f"Falha no login: {e}", "ERRO")
        driver.quit()
        raise

    # 3) Navegar até tela de Recebidos e aplicar filtros
    try:
        log("Navegando até A Receber...", "INFO")

        acoes_fluxo = [
            {"xpath": "//span[@class='icon fa fa-signal']", "descricao": "Ícone Contas"},
            {"xpath": "//span[@class='icon fa fa-hand-holding-usd']", "descricao": "Contas a Receber"},
            {"xpath": "//a[@href='#maintabRecebiveis-receber']", "descricao": "Aba Recebidos"},
            {"xpath": "//a[@href='#subtabRecebiveis-pesquisar']", "descricao": "Subaba Pesquisar"},
            {"xpath": "//span[@title='Mostrar Período']", "n": 0, "descricao": "Mostrar Período"},
            {"xpath": "//input[@name='ReceberDataVencimentoDataInicio']",  "acao": "digitar", "texto": data_inicio, "descricao": "Data Início"},
            {"xpath": "//input[@name='ReceberDataVencimentoDataTermino']", "acao": "digitar", "texto": data_fim,    "descricao": "Data Fim"},
            {"xpath": "//a[@id='Filtrar']", "n": 1, "descricao": "Botão Filtrar"},
        ]
        interagir_elementos(driver, acoes_fluxo)
        log("✅ Filtros aplicados com sucesso", "OK")
        time.sleep(0.4)

    except Exception as e:
        log(f"Falha na navegação/filtros: {e}", "ERRO")
        driver.quit()
        raise

    # 4) Download
    try:
        snap_antes = snapshot_downloads(pasta_download)
        acoes_download = [
            {"xpath": "//a[@title='Download em formato Excel']", "descricao": "Download Excel"},
            {"xpath": "//button[@class='swal2-confirm swal2-styled']", "descricao": "Confirmar Download"},
        ]
        interagir_elementos(driver, acoes_download)
        log("✅ Download iniciado", "OK")

        caminho_arquivo = aguardar_novo_download(
            pasta_download=pasta_download,
            snapshot_anterior=snap_antes,
            nome_substring=NOME_PADRAO_ARQUIVO,
            regex_nome=None,
            timeout=45,
            intervalo_polls=0.2,
        )
        log(f"[ETL] {os.path.basename(caminho_arquivo)}", "INFO")

        # Fecha navegador de forma assíncrona
        fechar_navegador_assincrono(driver, timeout=3.0)

    except Exception as e:
        log(f"Falha no download/verificação: {e}", "ERRO")
        driver.quit()
        raise

    # 5) Executa ETL + Upload BigQuery
    try:
        rodar_etl_generico(caminho_arquivo, ETL_CONFIG)
    except Exception as e:
        log(f"[ETL] Falha geral: {e}", "ERRO")
        raise
