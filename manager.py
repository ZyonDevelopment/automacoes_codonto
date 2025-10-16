"""
Gerenciador de Automações OdontoClean
------------------------------------
Orquestra as rotinas de automação (Expresso ou Personalizado)
usando as funções utilitárias definidas em `functions.py`.
"""

import sys
import os
from functions import (
    log,
    reset_tempo_base,
    obter_opcao_usuario,
    modo_expresso,
    modo_personalizado,
)

# Configuração de paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Importa as automações
from automations.valores_recebidos import executar_recebidos, ETL_CONFIG as ETL_RECEBIDOS

# Pasta padrão e credenciais
PASTA_DOWNLOADS = os.path.join(BASE_DIR, "downloads")
os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
log(f"Pasta de downloads global: {PASTA_DOWNLOADS}", "OK")

USUARIO_PADRAO = "isael.souza"
SENHA_PADRAO = "Odonto1234"

# Dicionário central de automações
AUTOMACOES = {
    "1": ("Recebidos", executar_recebidos, ETL_RECEBIDOS),
    # futuras automações:
    # "2": ("Pagamentos", executar_pagamentos, ETL_PAGAMENTOS),
    # "3": ("Contratos", executar_contratos, ETL_CONTRATOS),
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
