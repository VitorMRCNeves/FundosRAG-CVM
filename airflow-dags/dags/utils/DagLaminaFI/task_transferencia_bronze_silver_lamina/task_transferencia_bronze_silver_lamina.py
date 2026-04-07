import os
from dateutil.parser import isoparse
from deltalake import DeltaTable, write_deltalake
from utils.DagLaminaFI.task_transferencia_bronze_silver_lamina.schema import (
    SCHEMA_SILVER_LAMINA,
)
from utils.utilitarios import apply_schema

BUCKET_BRONZE = os.environ["BUCKET_BRONZE"]
BUCKET_SILVER = os.environ["BUCKET_SILVER"]


def fn_transferencia_bronze_silver_lamina(data_interval_end: str) -> None:
    """
    Publica snapshot lamina_fi Bronze → Silver.
    Seleciona o registro com max(timestamp_dagrun) → max(timestamp_escrita) da partição
    e sobrescreve a partição correspondente na Silver.

    Args:
        data_interval_end: Data/hora de referência (ISO).
    """
    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    ano = data_interval_end.year
    mes = data_interval_end.month

    df = DeltaTable(f"s3://{BUCKET_BRONZE}/fundos/laminas/lamina_fi").to_pandas(
        filters=[("ano_particao", "=", ano), ("mes_particao", "=", mes)]
    )

    df_max_run = df.loc[df["timestamp_dagrun"] == df["timestamp_dagrun"].max()]
    df_final = df_max_run.loc[
        df_max_run["timestamp_escrita"] == df_max_run["timestamp_escrita"].max()
    ]

    df_final = apply_schema(df_final, schema=SCHEMA_SILVER_LAMINA)

    path = f"s3://{BUCKET_SILVER}/fundos/laminas/lamina_fi"
    write_deltalake(
        path,
        df_final,
        partition_by=["ano_particao", "mes_particao"],
        mode="overwrite",
        partition_filters=[("ano_particao", "=", ano), ("mes_particao", "=", mes)],
    )
    dt = DeltaTable(path)
    dt.optimize.compact()
    dt.vacuum()
    dt.create_checkpoint()
    dt.cleanup_metadata()
