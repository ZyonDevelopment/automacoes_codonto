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
TABELA = "A_Receber"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
DIRETORIO_DOWNLOADS = os.path.join(ROOT_DIR, "automacoes_codonto", "downloads")
os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)

CREDENCIAIS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "GBOQ.json"))
NOME_PADRAO_ARQUIVO = "ControleODONTO - Títulos a Receber"

# =========================================================
# ========== CONFIGURAÇÃO DE ETL ===========================
# =========================================================
ETL_CONFIG = {
    # ======== Leitura ========
    "skip_top": 2,                  # pula cabeçalho extra do relatório
    "skip_bottom": 2,               # remove rodapé do arquivo
    "etl_especifico": "a_receber",  # módulo etl_a_receber.py
    "use_etl_pos": "",              # se não tiver pós-etl, pode deixar vazio

    # ======== Upload BigQuery ========
    "tabela": TABELA,               # ex: "A_Receber"
    "coluna_validacao": "valor_devido",  # usada p/ soma e checagem
    "periodo_coluna": "vencimento",      # coluna de datas para logs e deleção
    "limpar_periodo": True,              # NOVO: garante deleção do mesmo período antes do upload

    # ======== Extras / Compatibilidade ========
    "chaves_particao": [],          # mantido por compatibilidade futura
    "credenciais_path": CREDENCIAIS_PATH,
}


# =========================================================
# ========== FUNÇÃO PRINCIPAL ==============================
# =========================================================
def executar_a_receber(usuario, senha, data_inicio, data_fim, zoom=0.8, pasta_download=None):
    """
    Executa a automação de 'Valores A Receber'.
    Retorna o dicionário de resposta do ETL.
    """
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

        # Navegação e filtros
        acoes_fluxo = [
            {"xpath": "//span[@class='icon fa fa-signal']", "descricao": "Ícone Contas"},
            {"xpath": "//span[@class='icon fa fa-hand-holding-usd']", "descricao": "Contas a Receber"},
            {"xpath": "//a[@href='#maintabRecebiveis-receber']", "descricao": "Aba A Receber"},
            {"xpath": "//a[@href='#subtabRecebiveis-pesquisar']", "descricao": "Subaba Pesquisar"},
            {"xpath": "//span[@title='Mostrar Período']", "n": 0, "descricao": "Mostrar Período"},
            {"xpath": "//input[@name='ReceberDataVencimentoDataInicio']", "acao": "digitar", "texto": data_inicio, "descricao": "Data Início"},
            {"xpath": "//input[@name='ReceberDataVencimentoDataTermino']", "acao": "digitar", "texto": data_fim, "descricao": "Data Fim"},
            {"xpath": "//a[@id='Filtrar']", "n": 1, "descricao": "Botão Filtrar"},  # <<< n=1 aqui, confirmado
        ]
        interagir_elementos(driver, acoes_fluxo)
        time.sleep(0.4)

        # Download
        snap_antes = snapshot_downloads(pasta_download)
        acoes_download = [
            {"xpath": "//a[@title='Download em formato Excel']", "descricao": "Download Excel"},
            {"xpath": "//button[@class='swal2-confirm swal2-styled']", "descricao": "Confirmar Download"},
        ]
        interagir_elementos(driver, acoes_download)

        caminho_arquivo = aguardar_novo_download(
            pasta_download=pasta_download,
            snapshot_anterior=snap_antes,
            nome_substring=NOME_PADRAO_ARQUIVO,
            regex_nome=None,
            timeout=45,
            intervalo_polls=0.2,
        )
        fechar_navegador_assincrono(driver, timeout=3.0)

        # ETL e retorno
        resp = rodar_etl_generico(caminho_arquivo, ETL_CONFIG)
        return resp

    except Exception as e:
        log(f"❌ Falha geral em A_Receber: {e}", "ERRO")
        driver.quit()
        raise
