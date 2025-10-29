"""
TESTE — Mostrar Período (Contratos_Emitidos)
--------------------------------------------
Roda somente até o clique em 'Mostrar Período'
para identificar o índice correto do botão.
"""

import os
import sys
import time

# === Corrige o path para importar de automacoes_codonto ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
sys.path.append(ROOT_DIR)

from functions import (
    get_downloads_dir,
    iniciar_chrome,
    realizar_login_codonto,
    interagir_elementos,
    log,
    fechar_navegador_assincrono
)

# Selenium imports locais
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    TimeoutException,
)


def testar_mostrar_periodo(usuario, senha, zoom=0.8, pasta_download=None):
    """Teste interativo para encontrar o botão correto de 'Mostrar Período'."""

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
        log("✅ Login concluído, iniciando navegação até Contratos...", "INFO")

        # 2️⃣ Navegação até Contratos
        acoes_fluxo = [
            {"xpath": "//span[@class='icon fa fa-clock']", "descricao": "Ícone Relógio"},
            {"xpath": "//span[@class='icon fa fa-archive']", "descricao": "Movimentacoes"},
            {"xpath": "//a[@href='#maintabMovimentacao-contratos']", "descricao": "Contratos"},
        ]
        interagir_elementos(driver, acoes_fluxo)

        # 3️⃣ Diagnóstico de 'Mostrar Período'
        xpath_teste = "//span[@title='Mostrar Período']"
        log(f"🔍 Buscando elementos: {xpath_teste}", "INFO")

        elementos = WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.XPATH, xpath_teste))
        )

        log(f"🔎 {len(elementos)} elementos encontrados para '{xpath_teste}'", "INFO")

        for i, el in enumerate(elementos):
            try:
                txt = el.text.strip() or "(sem texto visível)"
                log(f"[TESTE] Tentando clique em índice {i} — texto: {txt}", "INFO")
                el.click()
                log(f"✅ Clique bem-sucedido com n={i}", "INFO")
                break
            except (ElementClickInterceptedException, ElementNotInteractableException):
                log(f"⚠️ Elemento {i} não interativo, tentando o próximo...", "WARN")
                time.sleep(1)
            except Exception as e:
                log(f"⚠️ Erro inesperado no índice {i}: {e}", "WARN")
                time.sleep(1)
        else:
            log("❌ Nenhum clique bem-sucedido em 'Mostrar Período'", "ERRO")

        # 🕒 pausa curta pra inspecionar navegador
        time.sleep(5)
        fechar_navegador_assincrono(driver, timeout=3.0)
        log("✅ Teste finalizado.", "INFO")

    except TimeoutException:
        log("❌ Timeout: elemento 'Mostrar Período' não encontrado.", "ERRO")
        driver.quit()
    except Exception as e:
        log(f"❌ Erro geral no teste: {e}", "ERRO")
        driver.quit()


# ==========================================================
# Execução direta
# ==========================================================
if __name__ == "__main__":
    testar_mostrar_periodo(
        usuario="isael.souza",
        senha="Odonto1234"
    )
