import sys
import os

# Garante acesso à raiz do projeto
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

"""
Gerenciador de Automações OdontoClean
------------------------------------
Orquestra as rotinas de automação (Expresso ou Personalizado)
usando as funções utilitárias definidas em `functions.py`.
"""

from functions import (
    log,
    reset_tempo_base,
    obter_opcao_usuario,
    modo_expresso,
    modo_personalizado,
    get_downloads_dir,
)

# === CONFIGURAÇÃO GERAL ===
PASTA_DOWNLOADS = get_downloads_dir()
os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
log(f"Pasta de downloads global: {PASTA_DOWNLOADS}", "OK")

USUARIO_PADRAO = "isael.souza"
SENHA_PADRAO = "Odonto1234"

# === IMPORTA AS AUTOMAÇÕES ===
from automations.valores_recebidos import executar_recebidos, ETL_CONFIG as ETL_RECEBIDOS
from automations.valores_a_receber import executar_a_receber, ETL_CONFIG as ETL_A_RECEBER

# === REGISTRO CENTRAL ===
AUTOMACOES = {
    "1": ("Recebidos", executar_recebidos, ETL_RECEBIDOS),
    "2": ("A_Receber", executar_a_receber, ETL_A_RECEBER),
    # futuras:
    # "3": ("Pagamentos", executar_pagamentos, ETL_PAGAMENTOS),
    # "4": ("Contratos", executar_contratos, ETL_CONTRATOS),
}


def menu_principal() -> None:
    """Exibe o menu principal e direciona para o modo escolhido."""
    log("=== GERENCIADOR DE AUTOMAÇÕES ODONTOCLEAN ===")
    print("\n[1] - Download Expresso")
    print("[2] - Download Personalizado")
    print("[0] - Sair")

    opcao = obter_opcao_usuario("\nSelecione o modo: ", ["0", "1", "2"])

    if opcao == "1":
        modo_expresso(AUTOMACOES, USUARIO_PADRAO, SENHA_PADRAO, PASTA_DOWNLOADS)
    elif opcao == "2":
        modo_personalizado(AUTOMACOES, USUARIO_PADRAO, SENHA_PADRAO, PASTA_DOWNLOADS)
    else:
        log("Encerrando execução...")


def main() -> None:
    reset_tempo_base()
    menu_principal()


if __name__ == "__main__":
    main()
