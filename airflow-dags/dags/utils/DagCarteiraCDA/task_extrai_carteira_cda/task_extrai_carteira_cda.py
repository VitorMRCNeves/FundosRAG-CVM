import io
import os
import zipfile
from dateutil.parser import isoparse

import pandas as pd
import requests
from airflow.exceptions import AirflowSkipException


_BLOCOS = [f"blc_{i}" for i in range(1, 9)]


def _fn_open_decode_csv(arquivo_zip: zipfile.ZipFile, nome_csv: str) -> str:
    raw = arquivo_zip.read(nome_csv)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin1")


def fn_extrai_carteira_cda(data_interval_end: str) -> None:
    """
    Baixa o ZIP mensal de carteiras CDA da CVM, extrai os 8 CSVs (BLC_1..BLC_8)
    e salva cada um na Landing particionada. Lança AirflowSkipException se HTTP 404.

    Args:
        data_interval_end: Data/hora de referência ISO para determinar ano/mês.
    """
    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    ano = data_interval_end.year
    mes = data_interval_end.strftime("%m")
    data_str = data_interval_end.strftime("%Y-%m-%d")

    url = f"https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{ano}{mes}.zip"
    response = requests.get(url)

    if response.status_code == 404:
        raise AirflowSkipException(
            f"Arquivo de carteiras CDA não disponível para {ano}-{mes} (HTTP 404)."
        )
    response.raise_for_status()

    buffer = io.BytesIO(response.content)
    arquivo_zip = zipfile.ZipFile(buffer)
    arquivos_no_zip = arquivo_zip.namelist()

    bucket_landing = os.environ["BUCKET_LANDING"]

    for bloco in _BLOCOS:

        bloco_upper = bloco.upper()
        nome_csv = f"cda_fi_{bloco_upper}_{ano}{mes}.csv"

        csv_no_zip = next((f for f in arquivos_no_zip if nome_csv in f), None)
        if csv_no_zip is None:
            print(
                f"Arquivo '{nome_csv}' não encontrado no ZIP. Disponíveis: {arquivos_no_zip}"
            )
            continue

        df = pd.read_csv(
            io.StringIO(_fn_open_decode_csv(arquivo_zip, csv_no_zip)),
            sep=";",
            converters={i: str for i in range(100)},
            engine="c",
            on_bad_lines="warn",
        )

        path = f"s3://{bucket_landing}/fundos/cda/{bloco}/ano={ano}/mes={mes}/{bloco}_{data_str}.csv"

        df.to_csv(path, index=False)
