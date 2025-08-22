import pandas as pd
import os 
import glob


def extrair_dados(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta,'*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

def calcular_kpi_total_de_venda(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

def carregar_dados(df: pd.DataFrame, format_saida: list):
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("dados.csv")
        if formato == 'parquet':
            df.to_parquet("dados.parquet")


def pipeline_usuario_final(pasta: str, formato_de_saida: list):
    ler_dados = extrair_dados(pasta)
    calcula_kpi = calcular_kpi_total_de_venda(ler_dados)
    carregar_dados(calcula_kpi,formato_de_saida)
