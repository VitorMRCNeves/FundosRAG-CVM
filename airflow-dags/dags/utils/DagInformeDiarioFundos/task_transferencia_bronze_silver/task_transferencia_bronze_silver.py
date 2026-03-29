import datetime
from deltalake import DeltaTable, write_deltalake
from utils.DagInformeDiarioFundos.task_transferencia_bronze_silver.schema import (
    SCHEMA_INFORME_FUNDOS,
)
from utils.utilitarios import apply_schema
from dateutil.parser import isoparse
import os

BUCKET_BRONZE = os.environ("BUCKET_BRONZE")
BUCKET_SILVER = os.environ("BUCKET_SILVER")


def fn_transferencia_bronze_silver(
    data_interval_end: str, corrige_mes_anterior: bool = False
):
    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    if corrige_mes_anterior:
        primeiro_dia_mes = data_interval_end.replace(day=1)
        data_interval_end = primeiro_dia_mes - datetime.timedelta(days=1)

    ano = data_interval_end.year
    mes = data_interval_end.month

    df = DeltaTable(f"s3://{BUCKET_BRONZE}/fundos/cvm/").to_pandas(
        filters=[("ano_particao", "=", ano), ("mes_particao", "=", mes)]
    )

    df_max = df.loc[df["timestamp_dagrun"] == df["timestamp_dagrun"].max()]

    df_max_escrita = df_max.loc[
        df_max["timestamp_escrita"] == df_max["timestamp_escrita"].max()
    ]

    df_max_escrita = apply_schema(df_max_escrita, schema=SCHEMA_INFORME_FUNDOS)

    path = f"s3://{BUCKET_SILVER}/fundos/cvm/"

    write_deltalake(
        path,
        df_max_escrita,
        partition_by=["ano_particao", "mes_particao"],
        mode="overwrite",
        partition_filters=[("ano_particao", "=", ano), ("mes_particao", "=", mes)],
    )
    dt = DeltaTable(path)

    dt.optimize.compact()
    dt.vacuum()
    dt.create_checkpoint()
    dt.cleanup_metadata()
