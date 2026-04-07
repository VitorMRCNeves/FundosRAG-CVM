import io
import os
import zipfile
from dateutil.parser import isoparse
import pandas as pd
import requests
from airflow.exceptions import AirflowSkipException


BUCKET_LANDING = os.environ["BUCKET_LANDING"]

_TABELAS = {
    "lamina_fi": lambda ano, mes: f"lamina_fi_{ano}{mes}.csv",
    "carteira": lambda ano, mes: f"lamina_fi_carteira_{ano}{mes}.csv",
    "rentab_ano": lambda ano, mes: f"lamina_fi_rentab_ano_{ano}{mes}.csv",
    "rentab_mes": lambda ano, mes: f"lamina_fi_rentab_mes_{ano}{mes}.csv",
}


def fn_open_decode_csv(arquivo_zip: io.BytesIO, csv_no_zip: str) -> bytes:
    csv_bytes = arquivo_zip.read(csv_no_zip)
    try:
        csv_text = csv_bytes.decode("utf-8")
    except UnicodeDecodeError:
        csv_text = csv_bytes.decode("latin1")

    return csv_text


def fn_extrai_lamina_fi(data_interval_end: str) -> None:
    """
    Baixa o ZIP mensal de lâminas da CVM, extrai os 4 CSVs e salva na Landing
    particionada por ano/mês. Lança AirflowSkipException se HTTP 404.

    Args:
        data_interval_end: Data/hora de referência (ISO) para determinar ano/mês.
    """
    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    ano = data_interval_end.year
    mes = data_interval_end.strftime("%m")
    data_str = data_interval_end.strftime("%Y-%m-%d")

    url = f"https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/lamina_fi_{ano}{mes}.zip"
    response = requests.get(url)

    if response.status_code == 404:
        raise AirflowSkipException(
            f"Arquivo de lâminas não disponível para {ano}-{mes} (HTTP 404)."
        )
    response.raise_for_status()

    buffer = io.BytesIO(response.content)
    arquivo_zip = zipfile.ZipFile(buffer)
    arquivos_no_zip = arquivo_zip.namelist()
    print(arquivos_no_zip)
    for tabela, nome_csv_fn in _TABELAS.items():
        nome_csv = nome_csv_fn(ano, mes)

        csv_no_zip = next((f for f in arquivos_no_zip if nome_csv in f), None)
        if csv_no_zip is None:
            raise FileNotFoundError(
                f"Arquivo '{nome_csv}' não encontrado no ZIP. Arquivos disponíveis: {arquivos_no_zip}"
            )

        print(f"Processando: {csv_no_zip}")

        df = pd.read_csv(
            io.StringIO(fn_open_decode_csv(arquivo_zip, csv_no_zip)),
            sep=";",
            converters={i: str for i in range(100)},
            engine="c",
            on_bad_lines="warn",
        )

        destino = f"s3://{BUCKET_LANDING}/fundos/laminas/{tabela}/ano={ano}/mes={mes}/{tabela}_{data_str}.csv"

        df.to_csv(
            destino,
            index=False,
        )
