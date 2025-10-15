import sys
import os

# Adiciona a pasta principal (BOTS 5.0) ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import time
from functions import iniciar_chrome, interagir_elementos, realizar_login_codonto

# =========================================================
# ========== CONFIGURAÇÕES ================================
# =========================================================
PROJETO = "api-para-sheets-433311"
DATASET = "OdontoClean_Dados"
TABELA = "Recebidos"
DIRETORIO_DOWNLOADS = r"/home/odonto/bots/downloads"
CREDENCIAIS_PATH = r"/home/odonto/bots/GBOQ.json"

# =========================================================
# ========== FUNÇÃO PRINCIPAL =============================
# =========================================================
def executar_recebidos(usuario: str, senha: str, inicio: str, fim: str, zoom: float = 0.8):
    """
    Executa a automação de valores recebidos até a etapa de download.
    """
    print("\n=== INICIANDO AUTOMAÇÃO: VALORES RECEBIDOS ===")

    # 1️⃣ Iniciar navegador
    driver = iniciar_chrome(
        url_inicial="https://codonto.aplicativo.net/",
        modo_headless=False,
        zoom=zoom
    )

    # 2️⃣ Login
    try:
        realizar_login_codonto(driver, usuario, senha)
    except Exception as e:
        print(f"[ERRO] Falha no login: {e}")
        driver.quit()
        return

    # 3️⃣ Navegar até a tela de Recebidos e aplicar filtros
    try:
        print("[INFO] Navegando até tela de Recebidos...")

        acoes_fluxo = [
            {"xpath": "//span[@class='icon fa fa-signal']", "descricao": "Ícone de Contas"},
            {"xpath": "//span[@class='icon fa fa-hand-holding-usd']", "descricao": "Contas A Receber"},
            {"xpath": "//a[@href='#maintabRecebiveis-recebidos']", "descricao": "Aba Recebidos"},
            {"xpath": "//a[@href='#subtabRecebidos-pesquisar']", "descricao": "Subaba Pesquisar"},
            {"xpath": "//span[@title='Mostrar Período']", "n": 0, "descricao": "Mostrar Período"},
            {"xpath": "//input[@name='RecebidoDataInicio']", "acao": "digitar", "texto": inicio, "descricao": "Data Início"},
            {"xpath": "//input[@name='RecebidoDataTermino']", "acao": "digitar", "texto": fim, "descricao": "Data Fim"},
            {"xpath": "//a[@id='Filtrar']", "n": 2, "descricao": "Botão Filtrar"},
        ]

        interagir_elementos(driver, acoes_fluxo)
        print("[INFO] Filtros aplicados com sucesso ✅")
        time.sleep(3)

    except Exception as e:
        print(f"[ERRO] Falha na navegação/filtros: {e}")
        driver.quit()
        return

    # 4️⃣ Exportar para Excel
    try:
        print("[INFO] Baixando relatório Excel...")

        acoes_download = [
            {"xpath": "//a[@title='Download em formato Excel']", "descricao": "Botão de Download Excel"},
            {"xpath": "//button[@class='swal2-confirm swal2-styled']", "descricao": "Confirmação do Download"}
            ]

        interagir_elementos(driver, acoes_download)
        print("[INFO] Download iniciado. Aguardando término...")
        time.sleep(15)

        print("[SUCESSO] Download concluído com sucesso ✅")

    except Exception as e:
        print(f"[ERRO] Falha no download: {e}")
        driver.quit()
        return

    # 5️⃣ Encerrar navegador
    driver.quit()
    print("[INFO] Navegador encerrado.")
