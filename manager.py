import sys
import os
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

from functions import gerar_periodos, snapshot_downloads, aguardar_novo_download

# Adiciona a pasta raiz e automations ao sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "automations"))
sys.path.append(os.path.join(BASE_DIR, "etl"))  # 🔧 inclui pasta de ETL

from automations import valores_recebidos
from automations.valores_recebidos import executar_recebidos, ETL_CONFIG  # 🔧 importa também a config ETL
# Ajusta o caminho do módulo ETL corretamente
sys.path.append(os.path.join(BASE_DIR, "..", "etl"))
from etl_manager import rodar_etl_generico

# =========================================================
# ========== CONFIGURAÇÃO DE PASTA DE DOWNLOADS ============
# =========================================================
PASTA_DOWNLOADS = os.path.join(BASE_DIR, "downloads")
os.makedirs(PASTA_DOWNLOADS, exist_ok=True)

print(f"[INFO] Pasta de downloads global: {PASTA_DOWNLOADS}")

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

        periodos = gerar_periodos(data_inicio, data_fim, meses_por_bloco=6)

        for i, (inicio, fim) in enumerate(periodos, 1):
            print(f"\n[✔] {inicio} → {fim}\n{'-'*60}")


            # Snapshot antes do download
            snapshot_antes = snapshot_downloads(PASTA_DOWNLOADS)

            try:
                # 1️⃣ Executa automação de download
                executar_recebidos(
                    usuario,
                    senha,
                    inicio.strftime("%d/%m/%Y"),
                    fim.strftime("%d/%m/%Y"),
                    zoom=0.8,
                    pasta_download=PASTA_DOWNLOADS
                )

                # 2️⃣ Aguarda o novo arquivo aparecer
                caminho_arquivo = aguardar_novo_download(
                    pasta_download=PASTA_DOWNLOADS,
                    snapshot_anterior=snapshot_antes,
                    nome_substring=valores_recebidos.NOME_PADRAO_ARQUIVO,
                    regex_nome=getattr(valores_recebidos, "PADRAO_ARQUIVO_REGEX", None),
                    timeout=240,
                    intervalo_polls=2.0,
                    espera_estabilidade=2.0
                )

                print(f"[OK] Arquivo detectado: {caminho_arquivo}")

                # 3️⃣ Executa o ETL genérico com base na configuração da automação
                print("[INFO] Iniciando ETL de transformação...")
                caminho_etl_final = rodar_etl_generico(caminho_arquivo, ETL_CONFIG)
                print(f"[OK] ETL concluído e salvo em: {caminho_etl_final}")

                print(f"[✅] Etapa completa para o período {inicio} a {fim}\n")

            except Exception as e:
                print(f"[ERRO] Falha ao processar período {inicio} a {fim}: {e}\n")

    elif opcao == "0":
        print("Encerrando...")
    else:
        print("Opção inválida.")


if __name__ == "__main__":
    main()
