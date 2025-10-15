# automacoes_codonto/manager.py
import sys, os, time
from datetime import datetime
from dateutil.relativedelta import relativedelta

from functions import log, reset_tempo_base, gerar_periodos

# paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))     # .../automacoes_codonto
ROOT_DIR = os.path.dirname(BASE_DIR)                      # raiz do projeto
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from automations.valores_recebidos import executar_recebidos, ETL_CONFIG
from etl.etl_manager import rodar_etl_generico

PASTA_DOWNLOADS = os.path.join(BASE_DIR, "downloads")
os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
log(f"Pasta de downloads global: {PASTA_DOWNLOADS}", "OK")


def main():
    log("=== GERENCIADOR DE AUTOMAÇÕES ODONTOCLEAN ===")
    log("1 - Executar Recebidos")
    log("0 - Sair")

    opcao = input("Selecione a opção desejada: ").strip()

    if opcao == "1":
        usuario = "isael.souza"
        senha = "Odonto1234"

        hoje = datetime.today().strftime("%d/%m/%Y")
        data_inicio = input(f"Data início (padrão 01/{hoje[3:]}): ").strip() or f"01/{hoje[3:]}"
        data_fim    = input(f"Data fim (padrão {hoje}): ").strip() or hoje

        log(f"Intervalo solicitado: {data_inicio} → {data_fim}")

        periodos = gerar_periodos(data_inicio, data_fim, meses_por_bloco=6)

        for i, (inicio, fim) in enumerate(periodos, 1):
            log(f"[{i}/{len(periodos)}] Período {inicio} → {fim}")

            try:
                # --- PROFILING: tempo total da automação (download + fechar navegador)
                t_auto_ini = time.time()
                caminho_arquivo = executar_recebidos(
                    usuario,
                    senha,
                    inicio.strftime("%d/%m/%Y"),
                    fim.strftime("%d/%m/%Y"),
                    zoom=0.8,
                    pasta_download=PASTA_DOWNLOADS,
                )
                t_auto_fim = time.time()
                #log(f"[TESTE] executar_recebidos retornou em +{t_auto_fim - t_auto_ini:.2f}s, arquivo: {caminho_arquivo}", "OK")

                # --- PROFILING: ETL
                #log("Iniciando ETL de transformação...")
                t_etl_ini = time.time()
                caminho_etl_final = rodar_etl_generico(caminho_arquivo, ETL_CONFIG)
                t_etl_fim = time.time()
                #log(f"[TESTE] ETL levou +{t_etl_fim - t_etl_ini:.2f}s → {caminho_etl_final}", "OK")

                #log(f"✅ Etapa finalizada: {inicio} → {fim}", "OK")

            except Exception as e:
                log(f"Falha ao processar {inicio} → {fim}: {e}", "ERRO")

    elif opcao == "0":
        log("Encerrando execução...")
    else:
        log("Opção inválida.", "WARN")


if __name__ == "__main__":
    reset_tempo_base()
    main()
