from dateutil.parser import isoparse
import requests
import pandas as pd
import zipfile
import io
from datetime import timedelta
import os

BUCKET_LANDING = os.environ("BUCKET_LANDING")


def fn_get_informes_diarios(
    data_interval_end: str, corrige_mes_anterior: bool = False
) -> None:
    """
    Esta função extrai o relatório diário de cotas de fundos da CVM abre o ZIP, e passa para um csv,
    o salvando na landing do S3, organizando o download por ano e por mês.
    Args:
        data_interval_end (str): Data de execução da dag no Airflow
    Returns:
        None
    """
    bucket_name = "bucket-landing"

    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    if corrige_mes_anterior:
        primeiro_dia_mes = data_interval_end.replace(day=1)
        data_interval_end = primeiro_dia_mes - timedelta(days=1)

    ano = data_interval_end.year
    mes = data_interval_end.strftime("%m")

    response = requests.get(
        f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ano}{mes}.zip"
    )

    if response.status_code == 200:

        buffer = io.BytesIO(response.content)

        arquivo_zip = zipfile.ZipFile(buffer)

        csv_file = pd.read_csv(
            arquivo_zip.open(arquivo_zip.namelist()[0]), sep=";", decimal="."
        )

        csv_file.to_csv(
            f"s3://{BUCKET_LANDING}/fundos/cvm/ano={ano}/mes={mes}/informes_{data_interval_end.strftime('%Y-%m-%d')}.csv",
            index=False,
        )
