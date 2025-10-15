import sys
import os
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from functions import gerar_periodos
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

        print(f"\n[INFO] Intervalo solicitado: {data_inicio} até {data_fim}")
        print("[INFO] Dividindo em blocos de até 6 meses...\n")

        periodos = gerar_periodos(data_inicio, data_fim, meses_por_bloco=6)

        for i, (inicio, fim) in enumerate(periodos, 1):
            print(f"\n=== Rodada {i}/{len(periodos)}: {inicio} até {fim} ===")
            try:
                executar_recebidos(usuario, senha, inicio.strftime("%d/%m/%Y"), fim.strftime("%d/%m/%Y"), zoom=0.8)
                print(f"[OK] ETL concluído para o período {inicio} a {fim}\n")
            except Exception as e:
                print(f"[ERRO] Falha ao processar período {inicio} a {fim}: {e}\n")

    elif opcao == "0":
        print("Encerrando...")
    else:
        print("Opção inválida.")


if __name__ == "__main__":
    main()
