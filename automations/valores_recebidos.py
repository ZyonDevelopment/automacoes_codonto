import sys
import os
import time

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
TABELA = "Recebidos_Codonto"

# diretórios no servidor
DIRETORIO_DOWNLOADS = r"/home/odonto/bots/downloads"
CREDENCIAIS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "GBOQ.json")
)


# nome esperado no arquivo
NOME_PADRAO_ARQUIVO = "ControleODONTO Fluxo de Caixa"

# =========================================================
# ========== CONFIGURAÇÃO DE ETL (NOVO FORMATO) ===========
# =========================================================
ETL_CONFIG = {
    # parâmetros de leitura
    "skip_top": 0,
    "skip_bottom": 2,

    # função ETL específica
    "etl_especifico": "recebidos",
    "use_etl_pos": "etl_recebidos_pos",

    # parâmetros de upload BigQuery
    "tabela": TABELA,
    "coluna_validacao": "valor_recebido",        # usada na soma de validação
    "periodo_coluna": "data",                    # define o recorte de período
    "chaves_particao": [], # campos usados no DELETE

    # caminho de credenciais (opcional)
    "credenciais_path": CREDENCIAIS_PATH,
}

# =========================================================
# ========== FUNÇÃO PRINCIPAL DA AUTOMAÇÃO ================
# =========================================================
def executar_recebidos(usuario, senha, data_inicio, data_fim, zoom=0.8, pasta_download=None):
    """
    Executa a automação de 'Valores Recebidos':
    - Faz login no Codonto.
    - Aplica filtros de data.
    - Baixa o relatório Excel.
    - Executa o ETL e faz upload ao BigQuery.
    """
    print("\n=== INICIANDO AUTOMAÇÃO: VALORES RECEBIDOS ===")

    if not pasta_download:
        raise ValueError("É obrigatório informar 'pasta_download' para salvar o relatório.")

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
        log("Navegando até Recebidos...", "INFO")

        acoes_fluxo = [
            {"xpath": "//span[@class='icon fa fa-signal']", "descricao": "Ícone Contas"},
            {"xpath": "//span[@class='icon fa fa-hand-holding-usd']", "descricao": "Contas a Receber"},
            {"xpath": "//a[@href='#maintabRecebiveis-recebidos']", "descricao": "Aba Recebidos"},
            {"xpath": "//a[@href='#subtabRecebidos-pesquisar']", "descricao": "Subaba Pesquisar"},
            {"xpath": "//span[@title='Mostrar Período']", "n": 0, "descricao": "Mostrar Período"},
            {"xpath": "//input[@name='RecebidoDataInicio']",  "acao": "digitar", "texto": data_inicio, "descricao": "Data Início"},
            {"xpath": "//input[@name='RecebidoDataTermino']", "acao": "digitar", "texto": data_fim,    "descricao": "Data Fim"},
            {"xpath": "//a[@id='Filtrar']", "n": 2, "descricao": "Botão Filtrar"},
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
