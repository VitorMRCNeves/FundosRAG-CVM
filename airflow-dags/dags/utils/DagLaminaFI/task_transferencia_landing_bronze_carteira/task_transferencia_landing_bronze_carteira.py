import os
from datetime import datetime
from dateutil.parser import isoparse

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from utils.DagLaminaFI.task_transferencia_landing_bronze_carteira.schema import (
    SCHEMA_BRONZE_CARTEIRA,
)
from utils.utilitarios import apply_schema

BASE_PATH = os.environ.get(
    "BASE_PATH",
    "s3://dev--dados-lake-landing-oneinvest/fundos/laminas/",
)

BUCKET_BRONZE = os.environ("BUCKET_BRONZE")
BUCKET_LANDING = os.environ("BUCKET_LANDING")


def fn_transferencia_landing_bronze_carteira(data_interval_end: str) -> None:
    """
    Lê o CSV da tabela carteira na Landing, aplica schema Bronze e persiste em Delta Lake (append).

    Args:
        data_interval_end: Data/hora de referência (ISO).
    """
    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    ano = data_interval_end.year
    mes = data_interval_end.strftime("%m")
    data_str = data_interval_end.strftime("%Y-%m-%d")

    df = pd.read_csv(
        f"s3://{BUCKET_LANDING}/fundos/laminas/carteira/ano={ano}/mes={mes}/carteira_{data_str}.csv",
        sep=",",
        decimal=".",
        dtype=str,
    )

    df["ano_particao"] = ano
    df["mes_particao"] = int(mes)
    df["timestamp_dagrun"] = data_interval_end
    df["timestamp_escrita"] = datetime.now()

    df = apply_schema(
        df=df, schema=SCHEMA_BRONZE_CARTEIRA, rename_cols=True, select_cols=True
    )

    path = f"s3://{BUCKET_BRONZE}/fundos/laminas/carteira/"
    write_deltalake(
        path,
        df,
        partition_by=["ano_particao", "mes_particao"],
        mode="append",
    )
    dt = DeltaTable(path)
    dt.optimize.compact()
    dt.vacuum()
    dt.create_checkpoint()
    dt.cleanup_metadata()
