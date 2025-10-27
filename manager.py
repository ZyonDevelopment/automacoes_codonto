import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Adiciona o diretório raiz (BOTS 5.0) ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

"""
Gerenciador de Automações OdontoClean
------------------------------------
Orquestra as rotinas de automação (Expresso ou Personalizado)
usando as funções utilitárias definidas em `functions.py`.
"""
from functions import (
    log, reset_tempo_base, obter_opcao_usuario,
    modo_expresso, modo_personalizado, get_downloads_dir
)

# Em vez de montar na mão:
# BASE_DIR = ...
# PASTA_DOWNLOADS = os.path.join(BASE_DIR, "downloads")
# os.makedirs(PASTA_DOWNLOADS, exist_ok=True)

PASTA_DOWNLOADS = get_downloads_dir()
log(f"Pasta de downloads global: {PASTA_DOWNLOADS}", "OK")

from functions import (
    log,
    reset_tempo_base,
    obter_opcao_usuario,
    modo_expresso,
    modo_personalizado,
)

# Importa as automações
from automations.valores_recebidos import executar_recebidos, ETL_CONFIG as ETL_RECEBIDOS
from automations.valores_a_receber import executar_a_receber, ETL_CONFIG as ELT_A_RECEBER
# Pasta padrão e credenciais
os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
log(f"Pasta de downloads global: {PASTA_DOWNLOADS}", "OK")

USUARIO_PADRAO = "isael.souza"
SENHA_PADRAO = "Odonto1234"

# Dicionário central de automações
AUTOMACOES = {
    "1": ("Recebidos", executar_recebidos, ETL_RECEBIDOS),
    "2": ("A_Receber", executar_a_receber, ELT_A_RECEBER),
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
