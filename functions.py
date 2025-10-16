"""
Módulo: functions.py
Descrição:
    Funções utilitárias centrais para automações OdontoClean.
    Inclui controle de logs, manipulação de arquivos, Selenium, períodos e login.
"""
from typing import Callable, Dict
from datetime import datetime
from typing import Callable, Dict
from datetime import datetime
import os, time
from typing import Callable, Dict
from datetime import datetime
import os, time
import os
import re
import time
import subprocess
from datetime import datetime
from threading import Thread
from typing import Dict, Set, Optional, List, Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
)

# =========================================================
# ========== LOGS E TEMPORIZAÇÃO ==========================
# =========================================================

_START_TIME = time.time()

def reset_tempo_base() -> None:
    """Reseta o tempo base para medição de logs."""
    global _START_TIME
    _START_TIME = time.time()


def log(msg: str, tipo: str = "INFO") -> None:
    """Exibe logs padronizados com tempo decorrido e ícones.

    Args:
        msg: Mensagem a ser exibida.
        tipo: Tipo do log ("INFO", "OK", "WARN", "ERRO").
    """
    elapsed = time.time() - _START_TIME
    prefix = f"[{elapsed:05.1f}s]"
    tipo = tipo.upper()

    icons = {
        "ERRO": "❌",
        "WARN": "⚠️ ",
        "OK": "✅",
    }
    simbolo = icons.get(tipo, "")
    print(f"{prefix} {simbolo} {msg}")


# =========================================================
# ========== FECHAMENTO DE NAVEGADOR =======================
# =========================================================

def _quit_driver(driver) -> None:
    """Tenta encerrar o WebDriver com segurança."""
    try:
        driver.quit()
    except Exception:
        pass


def fechar_navegador(driver, timeout: float = 3.0) -> None:
    """Fecha o navegador Selenium com fallback para kill de processo.

    Args:
        driver: Instância WebDriver.
        timeout: Tempo máximo de espera antes do kill.
    """
    t = Thread(target=_quit_driver, args=(driver,), daemon=True)
    t.start()
    t.join(timeout)

    if t.is_alive():
        pid = getattr(driver.service.process, "pid", None)
        if pid:
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


def fechar_navegador_assincrono(driver, timeout: float = 3.0) -> None:
    """Dispara o fechamento do navegador em background."""
    Thread(target=fechar_navegador, args=(driver, timeout), daemon=True).start()


# =========================================================
# ========== GERENCIAMENTO DE DOWNLOADS ===================
# =========================================================

TEMP_SUFFIXES = (".crdownload", ".part", ".download", ".tmp")

def _listar_arquivos_validos(pasta: str) -> Dict[str, float]:
    """Lista arquivos válidos (não temporários) e seus mtimes."""
    arquivos = {}
    with os.scandir(pasta) as it:
        for entry in it:
            if entry.is_file() and not entry.name.lower().endswith(TEMP_SUFFIXES):
                try:
                    arquivos[entry.name] = entry.stat().st_mtime
                except FileNotFoundError:
                    continue
    return arquivos


def snapshot_downloads(pasta_download: str) -> Set[str]:
    """Captura o estado atual dos arquivos válidos na pasta."""
    return set(_listar_arquivos_validos(pasta_download).keys())


def _match_por_regex_ou_substring(nome: str, regex: Optional[str], substring: Optional[str]) -> bool:
    """Verifica se o nome casa com regex ou contém substring."""
    if regex and re.search(regex, nome):
        return True
    if substring and substring.lower() in nome.lower():
        return True
    return False


def aguardar_novo_download(
    pasta_download: str,
    snapshot_anterior: Set[str],
    nome_substring: Optional[str] = None,
    regex_nome: Optional[str] = None,
    timeout: int = 45,
    intervalo_polls: float = 0.1,
) -> str:
    """Aguarda até que um novo arquivo (não temporário) apareça na pasta.

    Args:
        pasta_download: Caminho da pasta de downloads.
        snapshot_anterior: Snapshot anterior para comparação.
        nome_substring: Filtro de nome parcial (opcional).
        regex_nome: Filtro regex de nome (opcional).
        timeout: Tempo máximo em segundos.
        intervalo_polls: Intervalo entre verificações.

    Returns:
        Caminho completo do novo arquivo detectado.

    Raises:
        TimeoutError: Se nenhum arquivo novo for detectado dentro do limite.
    """
    t0 = time.time()
    snap = set(snapshot_anterior)

    while True:
        if time.time() - t0 > timeout:
            raise TimeoutError("Tempo limite aguardando novo download compatível.")

        atuais_dict = _listar_arquivos_validos(pasta_download)
        atuais = set(atuais_dict.keys())

        if atuais != snap:
            novos = [n for n in atuais if n not in snap]
            if not novos:
                snap = atuais
                time.sleep(intervalo_polls)
                continue

            candidatos = [n for n in novos if _match_por_regex_ou_substring(n, regex_nome, nome_substring)]
            if not candidatos:
                snap = atuais
                time.sleep(intervalo_polls)
                continue

            candidatos.sort(key=lambda n: atuais_dict.get(n, 0.0), reverse=True)
            escolhido = candidatos[0]
            caminho = os.path.join(pasta_download, escolhido)
            #log(f"✅ Arquivo detectado: {escolhido}", "OK")
            return caminho

        time.sleep(intervalo_polls)


# =========================================================
# ========== CONTROLE DE PERÍODOS ==========================
# =========================================================

def gerar_periodos(data_inicial: str, data_final: str, meses_por_bloco: int = 6) -> List[Tuple[datetime, datetime]]:
    """Divide um intervalo de datas em blocos de meses."""
    inicio = pd.to_datetime(data_inicial, dayfirst=True)
    fim = pd.to_datetime(data_final, dayfirst=True)
    periodos = []
    while inicio <= fim:
        proximo_fim = min(inicio + relativedelta(months=meses_por_bloco) - pd.Timedelta(days=1), fim)
        periodos.append((inicio.date(), proximo_fim.date()))
        inicio = proximo_fim + pd.Timedelta(days=1)
    return periodos


def obter_periodo_usuario(pergunta_tipo: bool = True) -> Tuple[datetime, datetime]:
    """Obtém o intervalo de datas conforme escolha do usuário, com validação robusta."""
    hoje = datetime.today()

    if not pergunta_tipo:
        return hoje.replace(day=1), hoje

    while True:
        print("\n📅 Escolha o período desejado:")
        print("1 - Mês atual")
        print("2 - Mês anterior")
        print("3 - Especificar manualmente")

        tipo = input("Selecione: ").strip()

        if tipo not in {"1", "2", "3"}:
            log("⚠️  Opção inválida, digite 1, 2 ou 3.", "WARN")
            continue

        if tipo == "1":
            return hoje.replace(day=1), hoje

        elif tipo == "2":
            mes_anterior = hoje - pd.DateOffset(months=1)
            data_inicio = mes_anterior.replace(day=1)
            ultimo_dia = (hoje.replace(day=1) - pd.Timedelta(days=1)).day
            data_fim = mes_anterior.replace(day=ultimo_dia)
            return data_inicio, data_fim

        else:
            # Loop de validação para datas manuais
            while True:
                try:
                    data_inicio_str = input("Data início (dd/mm/aaaa): ").strip()
                    data_inicio = datetime.strptime(data_inicio_str, "%d/%m/%Y")

                    data_fim_str = input("Data fim (dd/mm/aaaa): ").strip()
                    data_fim = datetime.strptime(data_fim_str, "%d/%m/%Y")

                    if data_inicio > data_fim:
                        log("⚠️  A data inicial não pode ser maior que a final.", "WARN")
                        continue

                    return data_inicio, data_fim

                except ValueError:
                    log("⚠️  Formato inválido. Use o formato dd/mm/aaaa (ex: 01/10/2025).", "WARN")
                    continue



def periodo_str(data_inicio: datetime, data_fim: datetime) -> str:
    """Formata o período em string para exibição em logs."""
    return f"{data_inicio.strftime('%d/%m/%Y')} → {data_fim.strftime('%d/%m/%Y')}"


# =========================================================
# ========== SELENIUM: INICIALIZAÇÃO =======================
# =========================================================

def iniciar_chrome(
    url_inicial: Optional[str] = None,
    modo_headless: bool = False,
    zoom: float = 1.0,
    pasta_download: Optional[str] = None
) -> webdriver.Chrome:
    """Inicia o navegador Chrome configurado para automações."""
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
            "safebrowsing.enabled": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
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
    try:
        driver.execute_script(f"document.body.style.zoom = '{zoom}'")
    except Exception:
        log("Falha ao aplicar zoom", "WARN")

    return driver


# =========================================================
# ========== SELENIUM: INTERAÇÕES ==========================
# =========================================================

def interagir_elementos(
    driver,
    acoes: List[Dict[str, Optional[str]]],
    max_retries: int = 3,
    timeout: int = 55,
    delay_apos_acao: float = 0.4
) -> None:
    """Executa múltiplas ações sequenciais em elementos Selenium."""
    avisos_xpaths = [
        "//button[@class='bt bt-primary bt-outline bt-small']",
        "//button[contains(@class,'swal2-confirm')]",
        "//button[text()='OK' or text()='Ok' or text()='ok']",
        "//div[@role='dialog']//button[@type='button']",
        "//button[contains(@class,'confirmar') or contains(.,'Confirmar')]",
    ]

    def fechar_avisos() -> None:
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
                        time.sleep(delay_apos_acao)
                        break
                    except (ElementClickInterceptedException, ElementNotInteractableException):
                        log(f"Tentativa {tentativa} falhou: {descricao} (bloqueado)", "WARN")
                        fechar_avisos()
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elemento)
                        time.sleep(0.3)
                        elemento.click()
                        time.sleep(delay_apos_acao)
                        break

                elif acao == "digitar":
                    elemento.clear()
                    elemento.send_keys(texto)
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

def realizar_login_codonto(driver, usuario: str, senha: str) -> None:
    """Executa login no sistema Codonto com credenciais fornecidas."""
    acoes_login = [
        {"xpath": "//input[@id='login']", "acao": "digitar", "texto": usuario, "descricao": "Campo Usuário"},
        {"xpath": "//input[@id='pass']", "acao": "digitar", "texto": senha, "descricao": "Campo Senha"},
        {"xpath": "//input[@id='checkTermsOfUse']", "descricao": "Termos de Uso"},
        {"xpath": "//button[@id='btnSubmit']", "acao": "clicar", "descricao": "Botão Entrar"},
    ]
    interagir_elementos(driver, acoes_login)
    time.sleep(2)

# =========================================================
# ========== FUNÇÕES DE INPUT VALIDADO ====================
# =========================================================
def obter_opcao_usuario(
    prompt: str,
    opcoes_validas: list[str],
    mensagem_erro: str = "Opção inválida, tente novamente."
) -> str:
    """Solicita uma entrada ao usuário e garante que seja uma das opções válidas.

    Args:
        prompt: Texto mostrado ao usuário.
        opcoes_validas: Lista de strings com as opções permitidas.
        mensagem_erro: Mensagem a exibir se o valor for inválido.

    Returns:
        A opção digitada, garantidamente válida.
    """
    while True:
        resposta = input(prompt).strip()
        if resposta in opcoes_validas:
            return resposta
        log(mensagem_erro, "WARN")


def obter_lista_de_opcoes(
    prompt: str,
    opcoes_validas: list[str],
    mensagem_erro: str = "Entrada inválida, tente novamente."
) -> list[str]:
    """Solicita ao usuário uma lista de números separados por vírgula e valida.

    Args:
        prompt: Texto mostrado ao usuário.
        opcoes_validas: Lista de opções numéricas válidas (ex: ['1','2','3']).
        mensagem_erro: Mensagem de erro em caso de opções fora do permitido.

    Returns:
        Lista de strings contendo as opções válidas digitadas.
    """
    while True:
        entrada = input(prompt).strip()
        if not entrada:
            log("Nenhuma opção digitada.", "WARN")
            continue

        escolhas = [x.strip() for x in entrada.split(",") if x.strip()]
        escolhas_validas = [e for e in escolhas if e in opcoes_validas]

        if not escolhas_validas:
            log(mensagem_erro, "WARN")
            continue

        return escolhas_validas

# =========================================================
# ========== GERENCIADORES DE EXECUÇÃO (MODOS) ============
# =========================================================

# =========================================================
# ========== UTILITÁRIO DE TEXTO PARA LOGS ================
# =========================================================
def periodo_str(data_inicio: datetime, data_fim: datetime) -> str:
    """Formata período em string curta."""
    return f"{data_inicio.strftime('%d/%m/%Y')} → {data_fim.strftime('%d/%m/%Y')}"


# =========================================================
# ========== EXECUÇÃO DE UMA AUTOMAÇÃO =====================
# =========================================================
def executar_automacao(
    nome: str,
    func_exec: Callable,
    etl_conf: dict,
    usuario: str,
    senha: str,
    data_inicio: datetime,
    data_fim: datetime,
    pasta_download: str
) -> None:
    """
    Executa uma automação completa (download + ETL + upload + limpeza de arquivos).

    Args:
        nome: Nome da automação (ex: 'Recebidos').
        func_exec: Função principal da automação.
        etl_conf: Dicionário de configuração do ETL.
        usuario: Usuário do sistema Codonto.
        senha: Senha do sistema Codonto.
        data_inicio: Data inicial do período.
        data_fim: Data final do período.
        pasta_download: Caminho da pasta de downloads.
    """
    from etl.etl_manager import rodar_etl_generico  # import local evita ciclo

    log(f"▶️  Iniciando {nome} — {periodo_str(data_inicio, data_fim)}")

    t_ini = time.time()
    try:
        # 1️⃣ Download e geração do arquivo bruto
        caminho_arquivo = func_exec(
            usuario,
            senha,
            data_inicio.strftime("%d/%m/%Y"),
            data_fim.strftime("%d/%m/%Y"),
            zoom=0.8,
            pasta_download=pasta_download,
        )

        # 2️⃣ ETL completo (gera _FINAL.xlsx e envia ao BigQuery)
        caminho_final = rodar_etl_generico(caminho_arquivo, etl_conf)

        # 3️⃣ Limpeza segura dos arquivos locais
        for caminho in [caminho_arquivo, caminho_final]:
            if caminho and os.path.exists(caminho):
                os.remove(caminho)
                log(f"🧹 Arquivo removido: {os.path.basename(caminho)}", "OK")
            else:
                log("⚠️  Nenhum arquivo encontrado para apagar.", "WARN")

        duracao = time.time() - t_ini
        log(f"✅ {nome} concluído em {duracao:.1f}s", "OK")

    except Exception as e:
        log(f"❌ Falha em {nome}: {e}", "ERRO")


# =========================================================
# ========== MODO EXPRESSO (TODAS AS AUTOMAÇÕES) ===========
# =========================================================
def modo_expresso(
    automacoes: Dict[str, tuple],
    usuario: str,
    senha: str,
    pasta_download: str
) -> None:
    """Executa todas as automações do mês atual."""
    log("🚀 Modo Expresso: executando todas as automações do mês atual")
    data_inicio, data_fim = obter_periodo_usuario(pergunta_tipo=False)

    for cod, (nome, func_exec, etl_conf) in automacoes.items():
        executar_automacao(nome, func_exec, etl_conf, usuario, senha, data_inicio, data_fim, pasta_download)


# =========================================================
# ========== MODO PERSONALIZADO (SELECIONAR AUTOMAÇÕES) ===
# =========================================================
def modo_personalizado(
    automacoes: Dict[str, tuple],
    usuario: str,
    senha: str,
    pasta_download: str
) -> None:
    """Executa automações selecionadas e período escolhido."""
    log("🧩 Modo Personalizado selecionado")

    print("\nAutomações disponíveis:")
    for cod, (nome, _, _) in automacoes.items():
        print(f"{cod} - {nome}")

    escolhidas = obter_lista_de_opcoes(
        "\nDigite os números das automações desejadas (ex: 1,3,5): ",
        list(automacoes.keys())
    )

    data_inicio, data_fim = obter_periodo_usuario(pergunta_tipo=True)
    log(f"Período selecionado: {periodo_str(data_inicio, data_fim)}")

    for cod in escolhidas:
        nome, func_exec, etl_conf = automacoes[cod]
        executar_automacao(nome, func_exec, etl_conf, usuario, senha, data_inicio, data_fim, pasta_download)
