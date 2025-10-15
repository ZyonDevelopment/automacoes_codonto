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
    fechar_navegador_assincrono,   # <-- novo
    log,
)


# =========================================================
# ========== CONFIGURAÇÕES =================================
# =========================================================
PROJETO = "api-para-sheets-433311"
DATASET = "OdontoClean_Dados"
TABELA = "Recebidos"

# servidor (local usamos a pasta do manager via argumento)
DIRETORIO_DOWNLOADS = r"/home/odonto/bots/downloads"
CREDENCIAIS_PATH    = r"/home/odonto/bots/GBOQ.json"

# Nome esperado no arquivo (pode ser substring)
NOME_PADRAO_ARQUIVO = "ControleODONTO Fluxo de Caixa"

# Config ETL compatível
ETL_CONFIG = {
    # antigas
    "skip_rows_top": 0,
    "skip_rows_bottom": 2,
    "use_etl_pos": "etl_recebidos_pos",

    # novas
    "skip_top": 0,
    "skip_bottom": 2,
    "etl_especifico": "recebidos",
}


# =========================================================
# ========== FUNÇÃO PRINCIPAL =============================
# =========================================================
def executar_recebidos(usuario, senha, data_inicio, data_fim, zoom=0.8, pasta_download=None):
    """
    Executa a automação de 'Valores Recebidos' e SÓ encerra o navegador
    depois de confirmar o download. Retorna o caminho do arquivo baixado.
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
    except Exception as e:
        log(f"Falha no login: {e}", "ERRO")
        driver.quit()
        raise

    # 3) Navegar até a tela e aplicar filtros
    try:
        log("Navegando até Recebidos...")

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
        #log("Filtros OK", "OK")
        time.sleep(0.4)  # micro pausa pro grid

    except Exception as e:
        log(f"Falha na navegação/filtros: {e}", "ERRO")
        driver.quit()
        raise

    # 4) Snapshot da pasta ANTES do clique de download
    try:
        snap_antes = snapshot_downloads(pasta_download)
        #log("Snapshot de downloads capturado")

        # dispare o download
        acoes_download = [
            {"xpath": "//a[@title='Download em formato Excel']", "descricao": "Download Excel"},
            {"xpath": "//button[@class='swal2-confirm swal2-styled']", "descricao": "Confirmar Download"},
        ]
        interagir_elementos(driver, acoes_download)
        #log("Download disparado", "OK")

        # aguarda criação do arquivo novo NA MESMA FUNÇÃO
        caminho_arquivo = aguardar_novo_download(
            pasta_download=pasta_download,
            snapshot_anterior=snap_antes,
            nome_substring=NOME_PADRAO_ARQUIVO,
            regex_nome=None,
            timeout=45,
            intervalo_polls=0.2,
            espera_estabilidade=0.2,
        )
        log(f"Arquivo confirmado: {caminho_arquivo}", "OK")

        # 5) Fechar o navegador em background (não bloqueia)
        fechar_navegador_assincrono(driver, timeout=3.0)
        #log("Fechando navegador em background...", "WARN")



    except Exception as e:
        log(f"Falha na etapa de download/verificação: {e}", "ERRO")
        driver.quit()
        raise
    
    # retorna o caminho do arquivo para o manager/etl
    return caminho_arquivo
