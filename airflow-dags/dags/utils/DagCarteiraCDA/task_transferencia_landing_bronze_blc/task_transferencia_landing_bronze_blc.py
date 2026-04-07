import os
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from dateutil.parser import isoparse

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.sh_schemas_bronze import (
    SCHEMAS_BRONZE,
)
from utils.utilitarios import apply_schema


def fn_processa_landing_bronze_blc(data_interval_end: str, bloco: str) -> None:
    """
    Lê o CSV do bloco CDA da Landing, aplica schema Bronze e persiste em Delta Lake (append).

    Args:
        data_interval_end: Data/hora de referência ISO (ano/mês de extração).
        bloco: Identificador do bloco, ex: "blc_1" ... "blc_8".
    """
    bucket_landing = os.environ["BUCKET_LANDING"]
    bucket_bronze = os.environ["BUCKET_BRONZE"]

    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    ano = data_interval_end.year
    mes = data_interval_end.strftime("%m")
    data_str = data_interval_end.strftime("%Y-%m-%d")

    path_landing = f"s3://{bucket_landing}/fundos/cda/{bloco}/ano={ano}/mes={mes}/{bloco}_{data_str}.csv"

    try:
        df = pd.read_csv(
            path_landing,
            sep=",",
            decimal=".",
            dtype=str,
        )
    except FileNotFoundError:
        raise AirflowSkipException

    df["ano_particao"] = ano
    df["mes_particao"] = int(mes)

    ts_dagrun = pd.Timestamp(data_interval_end)
    if ts_dagrun.tz is not None:
        ts_dagrun = ts_dagrun.tz_convert("UTC").tz_localize(None)
    df["timestamp_dagrun"] = ts_dagrun
    df["timestamp_escrita"] = datetime.now()

    df = apply_schema(
        df=df, schema=SCHEMAS_BRONZE[bloco], rename_cols=True, select_cols=True
    )

    path_bronze = f"s3://{bucket_bronze}/fundos/cda/{bloco}/"
    print(path_bronze)

    write_deltalake(
        path_bronze,
        df,
        partition_by=["ano_particao", "mes_particao"],
        mode="append",
    )
    dt = DeltaTable(path_bronze)
    dt.optimize.compact()
    dt.vacuum()
    dt.create_checkpoint()
    dt.cleanup_metadata()
