from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
)
import pandas as pd
from dateutil.relativedelta import relativedelta
import re
from typing import Dict, Set, Optional
# ====== Fechamento do navegador sem travar ======
import subprocess
from threading import Thread

def _quit_driver(driver):
    try:
        driver.quit()
    except Exception:
        pass

def fechar_navegador(driver, timeout: float = 3.0):
    """Fecha o Selenium graciosamente; se travar, mata o chromedriver (e filhos)."""
    t = Thread(target=_quit_driver, args=(driver,), daemon=True)
    t.start()
    t.join(timeout)

    if t.is_alive():
        pid = None
        try:
            pid = driver.service.process.pid
        except Exception:
            pass
        if pid is not None:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(pid), "/F", "/T"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                subprocess.run(["pkill", "-TERM", "-P", str(pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    os.kill(pid, 9)
                except Exception:
                    pass
        log("Navegador finalizado por kill (fallback)", "WARN")
    else:
        log("Navegador encerrado", "OK")

def fechar_navegador_assincrono(driver, timeout: float = 3.0):
    """Dispara o fechamento do navegador em background e retorna imediatamente."""
    Thread(target=fechar_navegador, args=(driver, timeout), daemon=True).start()

# =========================================================
# ========== SISTEMA DE LOG COM TEMPORIZAÇÃO ==============
# =========================================================
_START_TIME = time.time()

def reset_tempo_base():
    global _START_TIME
    _START_TIME = time.time()

def log(msg: str, tipo: str = "INFO"):
    elapsed = time.time() - _START_TIME
    prefix = f"[{elapsed:05.1f}s]"
    tipo = tipo.upper()
    if tipo == "ERRO":
        print(f"{prefix} ❌ {msg}")
    elif tipo == "WARN":
        print(f"{prefix} ⚠️  {msg}")
    elif tipo == "OK":
        print(f"{prefix} ✅ {msg}")
    else:
        print(f"{prefix} {msg}")


# =========================================================
# ========== FUNÇÕES DE ARQUIVOS / DOWNLOADS ==============
# =========================================================
TEMP_SUFFIXES = (".crdownload", ".part", ".download", ".tmp")
import os, re, time
from typing import Dict, Set, Optional

TEMP_SUFFIXES = (".crdownload", ".part", ".download", ".tmp")

def _listar_arquivos_validos(pasta: str) -> Dict[str, float]:
    """
    Lista arquivos válidos (não temporários) e seus mtimes de forma otimizada.
    Usa os.scandir() em vez de os.listdir() + getmtime, o que é 5–10x mais rápido.
    """
    arquivos = {}
    with os.scandir(pasta) as it:
        for entry in it:
            if not entry.is_file():
                continue
            nome = entry.name
            if nome.lower().endswith(TEMP_SUFFIXES):
                continue
            try:
                arquivos[nome] = entry.stat().st_mtime
            except FileNotFoundError:
                continue
    return arquivos


def snapshot_downloads(pasta_download: str) -> Set[str]:
    """
    Captura um snapshot rápido dos arquivos atuais válidos na pasta.
    Usado para comparar antes/depois de um novo download.
    """
    return set(_listar_arquivos_validos(pasta_download).keys())


def _match_por_regex_ou_substring(nome: str, regex: Optional[str], substring: Optional[str]) -> bool:
    """
    Retorna True se o nome casar com a regex OU contiver a substring.
    """
    if regex and re.search(regex, nome):
        return True
    if substring and substring.lower() in nome.lower():
        return True
    return False


def _arquivo_estavel(caminho: str, espera_estabilidade: float = 0.2) -> bool:
    """
    Verifica se o tamanho do arquivo não muda dentro de um intervalo curto.
    """
    try:
        tamanho_inicial = os.path.getsize(caminho)
        time.sleep(espera_estabilidade)
        tamanho_final = os.path.getsize(caminho)
        return tamanho_inicial == tamanho_final
    except (FileNotFoundError, PermissionError):
        return False
    
def aguardar_novo_download(
    pasta_download: str,
    snapshot_anterior: Set[str],
    nome_substring: Optional[str] = None,
    regex_nome: Optional[str] = None,
    timeout: int = 45,
    intervalo_polls: float = 0.1,
    # parâmetro mantido só por compatibilidade, mas ignorado:
    espera_estabilidade: float = 0.0,
) -> str:
    """
    Espera até a LISTA de arquivos válidos mudar em relação ao snapshot.
    Quando mudar, avalia apenas os ARQUIVOS NOVOS (não-temporários),
    filtra por substring/regex e retorna imediatamente o mais recente.
    (Sem checagem de 'estabilidade' de tamanho.)
    """
    #log(f"Aguardando novo download em: {pasta_download}")
    t0 = time.time()
    snap = set(snapshot_anterior)  # cópia para comparação

    while True:
        if time.time() - t0 > timeout:
            raise TimeoutError("Tempo limite aguardando novo download compatível.")

        # lista atual (só válidos: já ignora .crdownload/.part/.tmp)
        atuais_dict = _listar_arquivos_validos(pasta_download)  # {nome: mtime}
        atuais = set(atuais_dict.keys())

        if atuais != snap:
            # houve mudança na pasta
            novos = [n for n in atuais if n not in snap]
            if not novos:
                snap = atuais
                time.sleep(intervalo_polls)
                continue

            # aplica filtro (regex ou substring)
            candidatos = [n for n in novos if _match_por_regex_ou_substring(n, regex_nome, nome_substring)]
            if not candidatos:
                snap = atuais
                time.sleep(intervalo_polls)
                continue

            # retorna o mais recente por mtime
            candidatos.sort(key=lambda n: atuais_dict.get(n, 0.0), reverse=True)
            escolhido = candidatos[0]
            caminho = os.path.join(pasta_download, escolhido)
            log(f"✅ Arquivo detectado: {escolhido}", "OK")
            return caminho

        time.sleep(intervalo_polls)


# =========================================================
# ========== CONTROLE DE PERÍODOS ==========================
# =========================================================
def gerar_periodos(data_inicial: str, data_final: str, meses_por_bloco: int = 6):
    inicio = pd.to_datetime(data_inicial, dayfirst=True)
    fim = pd.to_datetime(data_final, dayfirst=True)
    periodos = []
    while inicio <= fim:
        proximo_fim = min(inicio + relativedelta(months=meses_por_bloco) - pd.Timedelta(days=1), fim)
        periodos.append((inicio.date(), proximo_fim.date()))
        inicio = proximo_fim + pd.Timedelta(days=1)
    return periodos


# =========================================================
# ========== NAVEGADOR SELENIUM ============================
# =========================================================
def iniciar_chrome(url_inicial: str = None, modo_headless: bool = False, zoom: float = 1.0, pasta_download: str = None):
    #log("Iniciando navegador Chrome...")

    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_experimental_option("prefs", {"profile.default_content_setting_values.notifications": 2})

    if modo_headless:
        chrome_options.add_argument("--headless=new")

    if pasta_download:
        os.makedirs(pasta_download, exist_ok=True)
        prefs = {
            "download.default_directory": os.path.abspath(pasta_download),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        #log(f"Pasta de download: {pasta_download}")
    else:
        log("Nenhuma pasta de download definida — usando padrão do sistema", "WARN")

    with open(os.devnull, 'w') as devnull:
        old_out, old_err = os.dup(1), os.dup(2)
        os.dup2(devnull.fileno(), 1)
        os.dup2(devnull.fileno(), 2)
        driver = webdriver.Chrome(options=chrome_options)
        os.dup2(old_out, 1)
        os.dup2(old_err, 2)

    if url_inicial:
        driver.get(url_inicial)
        #log(f"Acessando: {url_inicial}")
    else:
        log("Chrome iniciado sem URL")

    try:
        driver.execute_script(f"document.body.style.zoom = '{zoom}'")
        #log(f"Zoom definido para {zoom * 100:.0f}%")
    except Exception:
        log("Falha ao aplicar zoom", "WARN")

    return driver


# =========================================================
# ========== INTERAÇÃO COM ELEMENTOS =======================
# =========================================================
def interagir_elementos(driver, acoes: list, max_retries: int = 3, timeout: int = 25, delay_apos_acao: float = 0.4):
    avisos_xpaths = [
        "//button[@class='bt bt-primary bt-outline bt-small']",
        "//button[contains(@class,'swal2-confirm')]",
        "//button[text()='OK' or text()='Ok' or text()='ok']",
        "//div[@role='dialog']//button[@type='button']",
        "//button[contains(@class,'confirmar') or contains(.,'Confirmar')]",
    ]

    def fechar_avisos():
        for aviso_xpath in avisos_xpaths:
            try:
                btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, aviso_xpath)))
                btn.click()
                time.sleep(0.25)
                log(f"Aviso fechado: {aviso_xpath}", "WARN")
            except Exception:
                pass

    for item in acoes:
        xpath = item.get("xpath")
        acao = item.get("acao", "clicar")
        texto = item.get("texto")
        n = item.get("n")
        descricao = item.get("descricao", xpath)

        for tentativa in range(1, max_retries + 1):
            try:
                elementos = WebDriverWait(driver, min(timeout, 10)).until(
                    EC.presence_of_all_elements_located((By.XPATH, xpath))
                )
                elemento = elementos[n] if n is not None and n < len(elementos) else elementos[0]

                if acao == "clicar":
                    try:
                        elemento.click()
                        #log(f"{descricao}")
                        time.sleep(delay_apos_acao)
                        break
                    except (ElementClickInterceptedException, ElementNotInteractableException):
                        log(f"Tentativa {tentativa} falhou: {descricao} (bloqueado)", "WARN")
                        fechar_avisos()
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elemento)
                        time.sleep(0.3)
                        elemento.click()
                        #log(f"{descricao} (via scroll)")
                        time.sleep(delay_apos_acao)
                        break

                elif acao == "digitar":
                    elemento.clear()
                    elemento.send_keys(texto)
                    #log(f"digitou em {descricao}")
                    time.sleep(delay_apos_acao)
                    break

            except (TimeoutException, StaleElementReferenceException):
                log(f"Tentativa {tentativa} falhou: {descricao}", "WARN")
                fechar_avisos()
                if tentativa == max_retries:
                    log(f"Falha definitiva em {descricao}", "ERRO")
                    raise
                time.sleep(1)

            except Exception as e:
                log(f"Erro inesperado em {descricao}: {e}", "ERRO")
                fechar_avisos()
                if tentativa == max_retries:
                    raise


# =========================================================
# ========== LOGIN PADRÃO CODONTO ==========================
# =========================================================
def realizar_login_codonto(driver, usuario: str, senha: str):
    #log("Login no Codonto...")

    acoes_login = [
        {"xpath": "//input[@id='login']", "acao": "digitar", "texto": usuario, "descricao": "Campo Usuário"},
        {"xpath": "//input[@id='pass']", "acao": "digitar", "texto": senha, "descricao": "Campo Senha"},
        {"xpath": "//input[@id='checkTermsOfUse']", "descricao": "Termos de Uso"},
        {"xpath": "//button[@id='btnSubmit']", "acao": "clicar", "descricao": "Botão Entrar"},
    ]

    interagir_elementos(driver, acoes_login)
    #log("Login OK", "OK")
    time.sleep(2)
