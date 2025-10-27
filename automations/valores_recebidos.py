import sys
import os
import time
from functions import get_downloads_dir, log
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functions import (
    iniciar_chrome,
    interagir_elementos,
    realizar_login_codonto,
    snapshot_downloads,
    aguardar_novo_download,
    fechar_navegador_assincrono,
)
from etl.etl_manager import rodar_etl_generico

# =========================================================
# ========== CONFIGURAÇÕES GERAIS ==========================
# =========================================================
PROJETO = "api-para-sheets-433311"
DATASET = "Dados_OdontoClean"
TABELA = "Recebidos_Codonto"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
DIRETORIO_DOWNLOADS = os.path.join(ROOT_DIR, "automacoes_codonto", "downloads")
os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)

CREDENCIAIS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "GBOQ.json"))
NOME_PADRAO_ARQUIVO = "ControleODONTO Fluxo de Caixa"

# =========================================================
# ========== CONFIGURAÇÃO DE ETL ===========================
# =========================================================
ETL_CONFIG = {
    "skip_top": 0,
    "skip_bottom": 2,
    "etl_especifico": "recebidos",
    "use_etl_pos": "etl_recebidos_pos",
    "tabela": TABELA,
    "coluna_validacao": "valor_recebido",
    "periodo_coluna": "data",
    "chaves_particao": [],
    "credenciais_path": CREDENCIAIS_PATH,
}

# =========================================================
# ========== FUNÇÃO PRINCIPAL ==============================
# =========================================================
def executar_recebidos(usuario, senha, data_inicio, data_fim, zoom=0.8, pasta_download=None):
    """
    Executa a automação de 'Valores Recebidos'.
    Retorna o dicionário de resposta do ETL.
    """
    log("\n=== INICIANDO AUTOMAÇÃO: VALORES RECEBIDOS ===")

    if not pasta_download:
        pasta_download = get_downloads_dir()
        log("⚠️ pasta_download não informado — usando padrão.", "WARN")

    driver = iniciar_chrome(
        url_inicial="https://codonto.aplicativo.net/",
        zoom=zoom,
        pasta_download=pasta_download,
    )

    try:
        # Login
        realizar_login_codonto(driver, usuario, senha)
        log("✅ Login realizado com sucesso", "OK")

        # Navegação e filtros
        log("Navegando até Recebidos...", "INFO")
        acoes_fluxo = [
            {"xpath": "//span[@class='icon fa fa-signal']", "descricao": "Ícone Contas"},
            {"xpath": "//span[@class='icon fa fa-hand-holding-usd']", "descricao": "Contas a Receber"},
            {"xpath": "//a[@href='#maintabRecebiveis-recebidos']", "descricao": "Aba Recebidos"},
            {"xpath": "//a[@href='#subtabRecebidos-pesquisar']", "descricao": "Subaba Pesquisar"},
            {"xpath": "//span[@title='Mostrar Período']", "n": 0, "descricao": "Mostrar Período"},
            {"xpath": "//input[@name='RecebidoDataInicio']", "acao": "digitar", "texto": data_inicio, "descricao": "Data Início"},
            {"xpath": "//input[@name='RecebidoDataTermino']", "acao": "digitar", "texto": data_fim, "descricao": "Data Fim"},
            {"xpath": "//a[@id='Filtrar']", "n": 2, "descricao": "Botão Filtrar"},
        ]
        interagir_elementos(driver, acoes_fluxo)
        log("✅ Filtros aplicados com sucesso", "OK")
        time.sleep(0.4)

        # Download
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
        log(f"[ETL] Arquivo detectado: {os.path.basename(caminho_arquivo)}", "INFO")

        fechar_navegador_assincrono(driver, timeout=3.0)

        # ETL e retorno
        resp = rodar_etl_generico(caminho_arquivo, ETL_CONFIG)
        return resp

    except Exception as e:
        log(f"❌ Falha geral em Recebidos: {e}", "ERRO")
        driver.quit()
        raise
