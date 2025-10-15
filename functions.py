from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import pandas as pd
from dateutil.relativedelta import relativedelta
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
)
import re
from typing import Dict, Set, Optional

TEMP_SUFFIXES = (".crdownload", ".part", ".download", ".tmp")

def _listar_arquivos_validos(pasta: str) -> Dict[str, float]:
    """
    Retorna dict {nome_arquivo: mtime} ignorando arquivos temporários.
    """
    itens = {}
    for nome in os.listdir(pasta):
        # ignora temp de navegadores
        if nome.lower().endswith(TEMP_SUFFIXES):
            continue
        caminho = os.path.join(pasta, nome)
        if os.path.isfile(caminho):
            itens[nome] = os.path.getmtime(caminho)
    return itens

def snapshot_downloads(pasta_download: str) -> Set[str]:
    """Tira um snapshot (conjunto) dos nomes existentes e válidos na pasta."""
    return set(_listar_arquivos_validos(pasta_download).keys())

def _match_por_regex_ou_substring(nome: str, regex: Optional[str], substring: Optional[str]) -> bool:
    if regex:
        return re.search(regex, nome) is not None
    if substring:
        return substring.lower() in nome.lower()
    return False  # se nada foi fornecido, não casa

def _arquivo_estavel(caminho: str, espera_estabilidade: float = 2.0) -> bool:
    """
    Considera 'estável' se o tamanho não mudar dentro da janela espera_estabilidade.
    """
    try:
        tamanho1 = os.path.getsize(caminho)
        time.sleep(espera_estabilidade)
        tamanho2 = os.path.getsize(caminho)
        return tamanho1 == tamanho2
    except FileNotFoundError:
        return False

def aguardar_novo_download(
    pasta_download: str,
    snapshot_anterior: Set[str],
    nome_substring: Optional[str] = None,
    regex_nome: Optional[str] = None,
    timeout: int = 180,
    intervalo_polls: float = 1.5,
    espera_estabilidade: float = 2.0,
) -> str:
    """
    Aguarda até surgir UM NOVO arquivo (não-temp) na pasta, que case com
    regex_nome OU nome_substring, e esteja estável.
    Retorna o CAMINHO COMPLETO do arquivo encontrado.

    Estratégia:
    1) Compara o diretório com snapshot_anterior e pega apenas os 'novos'.
    2) Dentro dos novos, filtra por padrão (regex ou substring).
    3) Garante estabilidade (tamanho não cresce).
    4) Se múltiplos novos casarem, pega o mais recente por mtime.
    """
    print(f"[INFO] Aguardando novo download na pasta: {pasta_download}")
    inicio = time.time()

    while time.time() - inicio <= timeout:
        atuais = _listar_arquivos_validos(pasta_download)
        novos = [n for n in atuais.keys() if n not in snapshot_anterior]

        # filtra por padrão
        candidatos = [n for n in novos if _match_por_regex_ou_substring(n, regex_nome, nome_substring)]

        if candidatos:
            # escolhe o mais recente pelo mtime
            candidatos.sort(key=lambda n: atuais[n], reverse=True)
            escolhido = candidatos[0]
            caminho_escolhido = os.path.join(pasta_download, escolhido)

            # confirma estabilidade
            if _arquivo_estavel(caminho_escolhido, espera_estabilidade=espera_estabilidade):
                print(f"[OK] Novo arquivo detectado e estável: {escolhido}")
                return caminho_escolhido
            # se não estável ainda, continua polling
        time.sleep(intervalo_polls)

    raise TimeoutError(
        "[ERRO] Tempo limite esperando novo download compatível com o padrão. "
        "Verifique se o navegador realmente iniciou o download e o nome esperado."
    )


def gerar_periodos(data_inicial: str, data_final: str, meses_por_bloco: int = 6):
    """
    Divide o intervalo em blocos de meses (ex: 6 meses = semestre).
    Retorna lista de tuplas (inicio, fim).
    """
    inicio = pd.to_datetime(data_inicial)
    fim = pd.to_datetime(data_final)
    periodos = []

    while inicio <= fim:
        proximo_fim = min(inicio + relativedelta(months=meses_por_bloco) - pd.Timedelta(days=1), fim)
        periodos.append((inicio.date(), proximo_fim.date()))
        inicio = proximo_fim + pd.Timedelta(days=1)

    return periodos


def rodar_em_blocos(data_inicial, data_final, meses_por_bloco, func_execucao):
    """
    Controla a execução de qualquer função em blocos de tempo.
    Recebe:
      - data_inicial, data_final
      - meses_por_bloco (ex: 6)
      - func_execucao(inicio, fim): função que roda o ETL de cada bloco
    """
    periodos = gerar_periodos(data_inicial, data_final, meses_por_bloco)
    print(f"[INFO] Intervalo total {data_inicial} a {data_final}")
    print(f"[INFO] Dividido em {len(periodos)} blocos de {meses_por_bloco} meses cada.\n")

    for i, (inicio, fim) in enumerate(periodos, 1):
        print(f"[RODADA {i}] ETL de {inicio} até {fim} iniciado...")
        func_execucao(inicio, fim)
        print(f"[OK] Rodada {i} concluída ({inicio} a {fim}).\n")
        
def gerar_periodos(data_inicial: str, data_final: str, meses_por_bloco: int = 6):
    """
    Divide o intervalo informado em blocos fixos de meses (ex: 6 meses).
    Retorna uma lista de tuplas (inicio, fim).
    """
    inicio = pd.to_datetime(data_inicial, dayfirst=True)
    fim = pd.to_datetime(data_final, dayfirst=True)
    periodos = []

    while inicio <= fim:
        proximo_fim = min(inicio + relativedelta(months=meses_por_bloco) - pd.Timedelta(days=1), fim)
        periodos.append((inicio.date(), proximo_fim.date()))
        inicio = proximo_fim + pd.Timedelta(days=1)

    return periodos


def iniciar_chrome(
    url_inicial: str = None,
    modo_headless: bool = False,
    zoom: float = 1.0,
    pasta_download: str = None
):
    """
    Inicia o navegador Chrome com download controlado e configurações seguras.

    Parâmetros:
        url_inicial (str): URL opcional para abrir.
        modo_headless (bool): Define se o navegador será iniciado sem interface.
        zoom (float): Define o nível de zoom da página (1.0 = 100%, 0.8 = 80%, etc.)
        pasta_download (str): Caminho da pasta onde os downloads serão salvos.
    """
    print("[INFO] Iniciando navegador Chrome...")

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

    # ✅ Configuração de pasta de download segura
    if pasta_download:
        os.makedirs(pasta_download, exist_ok=True)
        prefs = {
            "download.default_directory": os.path.abspath(pasta_download),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        print(f"[INFO] Pasta de download configurada: {pasta_download}")
    else:
        print("[AVISO] Nenhuma pasta de download definida — Chrome usará a padrão do sistema.")

    # Redireciona logs do ChromeDriver para /dev/null (silêncio total)
    try:
        with open(os.devnull, 'w') as devnull:
            old_out, old_err = os.dup(1), os.dup(2)
            os.dup2(devnull.fileno(), 1)
            os.dup2(devnull.fileno(), 2)
            driver = webdriver.Chrome(options=chrome_options)
    finally:
        os.dup2(old_out, 1)
        os.dup2(old_err, 2)

    # Abre a URL inicial (se houver)
    if url_inicial:
        driver.get(url_inicial)
        print(f"[INFO] Acessando URL: {url_inicial}")
    else:
        print("[INFO] Chrome iniciado sem URL.")

    # Define o zoom desejado via JavaScript
    try:
        driver.execute_script(f"document.body.style.zoom = '{zoom}'")
        print(f"[INFO] Zoom definido para {zoom * 100:.0f}%")
    except Exception as e:
        print(f"[AVISO] Não foi possível definir o zoom: {e}")

    return driver

def interagir_elementos(driver, acoes: list, max_retries: int = 3, timeout: int = 25, delay_apos_acao: float = 0.4):
    """
    Executa uma sequência de interações definidas por dicionários.
    Tenta primeiro SEM scroll; em caso de erro, fecha avisos, FAZ SCROLL e tenta novamente.
    """

    avisos_xpaths = [
        "//button[@class='bt bt-primary bt-outline bt-small']",
        "//button[contains(@class,'swal2-confirm')]",
        "//button[text()='OK' or text()='Ok' or text()='ok']",
        "//div[@role='dialog']//button[@type='button']",
        "//button[contains(@class,'confirmar') or contains(.,'Confirmar')]",
    ]

    def fechar_avisos():
        """Fecha possíveis avisos/popup quando houver erro."""
        for aviso_xpath in avisos_xpaths:
            try:
                btn = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, aviso_xpath))
                )
                # sem scroll aqui; prioridade é fechar rápido
                btn.click()
                time.sleep(0.25)
                print(f"[INFO] Aviso fechado: {aviso_xpath}")
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
                # 1) aguarda presença (timeout curto por tentativa)
                elementos = WebDriverWait(driver, min(timeout, 10)).until(
                    EC.presence_of_all_elements_located((By.XPATH, xpath))
                )
                elemento = elementos[n] if n is not None and n < len(elementos) else elementos[0]

                # 2) tenta ação SEM scroll primeiro
                if acao == "clicar":
                    try:
                        elemento.click()
                        #print(f"[→] {descricao}")
                        time.sleep(delay_apos_acao)
                        break
                    except (ElementClickInterceptedException, ElementNotInteractableException) as e:
                        print(f"[WARN] Tentativa {tentativa} falhou em {descricao}: {type(e).__name__}. Fechando avisos e rolando até o elemento...")
                        fechar_avisos()
                        # 3) agora faz scroll e tenta novamente
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elemento)
                        time.sleep(0.25)
                        try:
                            WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                            elemento.click()
                            print(f"[INFO] Ação 'clicar' executada com sucesso: {descricao}")
                            time.sleep(delay_apos_acao)
                            break
                        except Exception as e2:
                            # deixa cair para retry global
                            time.sleep(0.8 * tentativa)

                elif acao == "digitar":
                    try:
                        elemento.clear()
                        elemento.send_keys(texto)
                        #print(f"[→] digitou em {descricao}")
                        time.sleep(delay_apos_acao)
                        break
                    except Exception as e:
                        print(f"[WARN] Tentativa {tentativa} falhou em {descricao}: erro ao digitar ({e}). Fechando avisos e rolando até o elemento...")
                        fechar_avisos()
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elemento)
                        time.sleep(0.25)
                        try:
                            elemento.clear()
                            elemento.send_keys(texto)
                            #print(f"[INFO] Ação 'digitar' executada com sucesso: {descricao}")
                            time.sleep(delay_apos_acao)
                            break
                        except Exception:
                            time.sleep(0.8 * tentativa)

                else:
                    raise ValueError(f"Ação inválida: {acao}")

            except (TimeoutException, StaleElementReferenceException) as e:
                #print(f"[WARN] Tentativa {tentativa} falhou em {descricao}: {type(e).__name__}. Fechando avisos...")
                fechar_avisos()
                time.sleep(1.1 * tentativa)
                if tentativa == max_retries:
                    driver.save_screenshot(f"erro_{int(time.time())}.png")
                    print(f"[ERRO] Falha ao interagir com {descricao}.")
                    raise e

            except Exception as e:
                print(f"[WARN] Tentativa {tentativa} falhou em {descricao}: erro inesperado ({e}). Fechando avisos...")
                fechar_avisos()
                time.sleep(1.1 * tentativa)
                if tentativa == max_retries:
                    driver.save_screenshot(f"erro_{int(time.time())}.png")
                    print(f"[ERRO] Falha inesperada ao interagir com {descricao}.")
                    raise e





def realizar_login_codonto(driver, usuario: str, senha: str):
    """
    Realiza o login no sistema Codonto usando interagir_elementos.
    """
    print("[INFO] Efetuando login no Codonto...")

    acoes_login = [
        {"xpath": "//input[@id='login']", "acao": "digitar", "texto": usuario, "descricao": "Campo Usuário"},
        {"xpath": "//input[@id='pass']", "acao": "digitar", "texto": senha, "descricao": "Campo Senha"},
        {"xpath": "//input[@id='checkTermsOfUse']","descricao":"Termos de Uso"},
        {"xpath": "//button[@id='btnSubmit']", "acao": "clicar", "descricao": "Botão Entrar"},
    ]

    interagir_elementos(driver, acoes_login)
    print("[INFO] Login realizado com sucesso ✅")
    time.sleep(2)
