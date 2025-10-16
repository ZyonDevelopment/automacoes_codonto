# teste_etl.py
import os
import re
import pandas as pd
import unicodedata

# ============= Helpers de texto =============
def remover_acentos(texto: str):
    if isinstance(texto, str):
        return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto

def normalizar_nome_coluna(col: str) -> str:
    """
    1) remove acentos
    2) lowercase
    3) troca separadores por _
    4) remove caracteres nao [a-z0-9_]
    5) colapsa underscores e strip
    """
    if not isinstance(col, str):
        col = str(col) if col is not None else "coluna"
    col = remover_acentos(col)
    col = col.lower().strip()
    col = re.sub(r"[\/\|\-\s]+", "_", col)              # separadores comuns -> _
    col = re.sub(r"[^a-z0-9_]", "", col)                # mantém só [a-z0-9_]
    col = re.sub(r"_+", "_", col).strip("_")            # colapsa _
    return col or "coluna"

def limpar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    novos = []
    vistos = {}
    for c in df.columns:
        base = normalizar_nome_coluna(c)
        # garante unicidade
        if base in vistos:
            vistos[base] += 1
            final = f"{base}_{vistos[base]}"
        else:
            vistos[base] = 0
            final = base
        novos.append(final)
    df.columns = novos
    return df

# ============= Conversão numérica =============
_NUM_LIKE_RE = re.compile(r"^\s*[-+]?[\d\.\,]+(?:\s*[%])?\s*$")

EXCLUIR_NUMERICAS = {
    "cpf", "cnpj", "cpf_cnpj", "cpfcnpj",
    "contrato", "telefone", "celular", "whatsapp",
    "doc", "documento", "rg", "inscricao", "matricula",
}

def _parece_numerico_series(s: pd.Series, min_ratio: float = 0.6) -> bool:
    """
    Diz se uma coluna de texto 'parece numerica' em >= min_ratio das linhas.
    """
    if s.dtype != "object":
        return False
    total = len(s)
    if total == 0:
        return False
    matches = s.astype(str).str.fullmatch(_NUM_LIKE_RE).sum()
    return (matches / total) >= min_ratio

def normalizar_numeros_coluna(s: pd.Series) -> pd.Series:
    """
    Remove milhares (.), troca vírgula decimal por ponto, remove R$, %, espaços,
    e converte para float com errors='coerce'.
    """
    # trabalha como string
    st = s.astype(str)

    # limpa símbolos comuns
    st = (
        st.str.replace(r"\s", "", regex=True)
          .str.replace("R$", "", regex=False)
          .str.replace("%", "", regex=False)
          .str.replace(".", "", regex=False)     # remove separador de milhar
          .str.replace(",", ".", regex=False)    # vírgula -> ponto decimal
    )

    # vazios -> NaN
    st = st.replace({"": None, "nan": None, "None": None})

    # converte
    return pd.to_numeric(st, errors="coerce")

def converter_colunas_numericas(df: pd.DataFrame) -> (pd.DataFrame, list):
    """
    Converte automaticamente colunas 'parece numerica' para float,
    respeitando EXCLUIR_NUMERICAS.
    Retorna (df, lista_de_colunas_convertidas).
    """
    convertidas = []
    for col in df.columns:
        if col in EXCLUIR_NUMERICAS:
            continue
        s = df[col]
        if s.dtype == "object" and _parece_numerico_series(s):
            df[col] = normalizar_numeros_coluna(s)
            convertidas.append(col)
    return df, convertidas

# ============= ETL base de teste =============
def etl_teste(caminho_arquivo: str) -> str:
    """
    Lê um Excel, aplica ETL base e salva no mesmo diretório com sufixo _ETL.
    Regras:
      - normaliza nomes de colunas SEM perder letras (nada de 'transao')
      - remove acentos do conteúdo apenas em colunas de texto
      - converte números no padrão BR -> float (., ,)
    """
    print(f"[INFO] Lendo arquivo: {caminho_arquivo}")
    df = pd.read_excel(caminho_arquivo, engine="openpyxl")
    print(f"[INFO] {len(df)} linhas carregadas.")

    # 1) normaliza nomes de colunas
    df = limpar_nomes_colunas(df)

    # 2) remove acentos APENAS de colunas texto (sem applymap deprecado)
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].map(remover_acentos)

    # 3) remove linhas completamente vazias
    df = df.dropna(how="all")

    # 4) converter numéricas automaticamente (com exclusões)
    df, cols_conv = converter_colunas_numericas(df)
    if cols_conv:
        print(f"[INFO] Colunas convertidas para float: {', '.join(cols_conv)}")
    else:
        print("[INFO] Nenhuma coluna elegível para conversão numérica automática.")

    # 5) NaN -> vazio em texto; mantém NaN em numéricas
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # mantém NaN (útil pro BQ)
            continue
        else:
            df[col] = df[col].fillna("")

    # 6) salva no mesmo diretório com sufixo _ETL
    diretorio = os.path.dirname(caminho_arquivo)
    nome = os.path.basename(caminho_arquivo)
    base, ext = os.path.splitext(nome)
    saida = os.path.join(diretorio, f"{base}_ETL{ext}")
    df.to_excel(saida, index=False, engine="openpyxl")
    print(f"[OK] ETL concluído. Arquivo salvo em:\n{saida}")
    return saida

# ============= Execução interativa =============
if __name__ == "__main__":
    caminho = r"C:\Users\OdontoClean\Downloads\ControleODONTO Fluxo de Caixa (28).xlsx"
    etl_teste(caminho)
