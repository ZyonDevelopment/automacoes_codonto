import sys
import os
from datetime import datetime

# Adiciona a pasta raiz e automations ao sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "automations"))

from automations.valores_recebidos import executar_recebidos


# =========================================================
# ========== GERENCIADOR DE EXECUÇÕES =====================
# =========================================================
def main():
    print("\n=== GERENCIADOR DE AUTOMAÇÕES ODONTOCLEAN ===")
    print("1 - Executar Recebidos")
    print("0 - Sair")

    opcao = input("Selecione a opção desejada: ").strip()

    if opcao == "1":
        usuario = input("Usuário Codonto: ").strip()
        senha = input("Senha Codonto: ").strip()

        hoje = datetime.today().strftime("%d/%m/%Y")
        data_inicio = input(f"Data início (padrão 01/{hoje[3:]}): ").strip() or f"01/{hoje[3:]}"
        data_fim = input(f"Data fim (padrão {hoje}): ").strip() or hoje

        print(f"\n[INFO] Executando automação de recebidos de {data_inicio} até {data_fim}...\n")
        executar_recebidos(usuario, senha, data_inicio, data_fim, zoom=0.8)

    elif opcao == "0":
        print("Encerrando...")
    else:
        print("Opção inválida.")


if __name__ == "__main__":
    main()
