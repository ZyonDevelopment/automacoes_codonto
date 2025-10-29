"""
AUTOMAÇÃO MODELO — OdontoClean
--------------------------------
Template para criar novas automações no padrão unificado.
Basta ajustar:
 - NOME_AUTOMACAO
 - ETL_CONFIG (tabela, colunas, etc)
 - XPATHs e descrições nas ações de navegação/download
"""

import sys
import os
import time
from functions import get_downloads_dir, log

# Garante acesso aos módulos
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
TABELA = "Contratos_Emitidos"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
DIRETORIO_DOWNLOADS = os.path.join(ROOT_DIR, "automacoes_codonto", "downloads")
os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)

CREDENCIAIS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "GBOQ.json")
)
NOME_PADRAO_ARQUIVO = "Contratos"  # exemplo: "ControleODONTO Pagamentos"

# =========================================================
# ========== CONFIGURAÇÃO DE ETL ===========================
# =========================================================
ETL_CONFIG = {
    # ======== Leitura ========
    "skip_top": 2,
    "skip_bottom": 0,
    "etl_especifico": "contratos",  # ex: "pagamentos", "contratos"
    "use_etl_pos": "",
    # ======== Upload BigQuery ========
    "tabela": TABELA,
    "coluna_validacao": "total_tratamento",  # ex: "valor_pago", "valor_contrato"
    "periodo_coluna": "emissao",          # ex: "data_pagamento", "data_contrato"
    "limpar_periodo": True,
    # ======== Extras ========
    "chaves_particao": [],
    "credenciais_path": CREDENCIAIS_PATH,
}


# =========================================================
# ========== FUNÇÃO PRINCIPAL ==============================
# =========================================================
def executar_contratos(usuario, senha, data_inicio, data_fim, zoom=0.8, pasta_download=None):
    """
    Executa a automação de 'NOME_AUTOMACAO'.
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
        # 1️⃣ Login
        realizar_login_codonto(driver, usuario, senha)
        acoes_antes = [ 
            {"xpath": "//span[@class='icon fa fa-clock']", "descricao": "Ícone Relógio"},
            {"xpath": "//span[@class='icon fa fa-archive']", "descricao": "Movimentacoes"},
        ]
        # 2️⃣ Navegação e filtros (ajustar XPATHs e descrições)
        acoes_fluxo = [
            {"xpath": "//span[@class='icon fa fa-clock']", "descricao": "Ícone Relógio"},
            {"xpath": "//span[@class='icon fa fa-archive']", "descricao": "Movimentacoes"},
            {"xpath": "//a[@href='#maintabMovimentacao-contratos']", "descricao": "Contratos"},
            {"xpath": "//span[@title='Mostrar Período']", "n":2, "descricao": "Mostrar Período"},
            {"xpath": "//input[@name='ContratoDataInicio']", "acao": "digitar", "texto": data_inicio, "descricao": "Data Início"},
            {"xpath": "//input[@name='ContratoDataTermino']", "acao": "digitar", "texto": data_fim, "descricao": "Data Fim"},
            {"xpath": "//a[@id='Filtrar']", "n": 2, "descricao": "Botão Filtrar"},
        ]
        interagir_elementos(driver, acoes_antes)
        driver.refresh()
        interagir_elementos(driver, acoes_fluxo)
        time.sleep(5.5)

        # 3️⃣ Download
        snap_antes = snapshot_downloads(pasta_download)
        acoes_download = [
            {"xpath": "//a[@title='Download em formato Excel']", "descricao": "Download Excel"},
        ]
        interagir_elementos(driver, acoes_download)

        caminho_arquivo = aguardar_novo_download(
            pasta_download=pasta_download,
            snapshot_anterior=snap_antes,
            nome_substring=NOME_PADRAO_ARQUIVO,
            timeout=45,
            intervalo_polls=0.3,
        )

        # 4️⃣ Fecha navegador e executa ETL
        fechar_navegador_assincrono(driver, timeout=3.0)
        resp = rodar_etl_generico(caminho_arquivo, ETL_CONFIG)

        return resp  # ⚡ essencial p/ limpeza automática funcionar

    except Exception as e:
        log(f"❌ Falha geral em {TABELA}: {e}", "ERRO")
        driver.quit()
        raise
