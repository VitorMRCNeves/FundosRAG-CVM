from datetime import datetime, timedelta
from dateutil.parser import isoparse
import pandas as pd
from utils.DagInformeDiarioFundos.task_transferencia_landing_bronze.schema import (
    SCHEMA_INFORME_FUNDOS,
)
from deltalake import write_deltalake, DeltaTable
from utils.utilitarios import apply_schema


def fn_leitura_base_landing(ano: int, mes: int, data_interval_end: datetime):
    """
    Encapsula a função de leitura da landing para facilitar o mock em testes
    """
    df = pd.read_csv(
        f"/root/pessoal/FundosRAG-CVM/airflow-dags/dags/utils/dados_informe_diario/fundos/landing/ano={ano}/mes={mes}/informes_{data_interval_end.strftime('%Y-%m-%d')}.csv",
        sep=",",
        decimal=".",
        converters={i: str for i in range(100)},
        engine="c",
    )
    return df


def fn_processa_landing_bronze(
    data_interval_end: str, corrige_mes_anterior: bool = False
):
    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    if corrige_mes_anterior:
        primeiro_dia_mes = data_interval_end.replace(day=1)
        data_interval_end = primeiro_dia_mes - timedelta(days=1)

    ano = data_interval_end.year
    mes = data_interval_end.strftime("%m")

    df_landing = fn_leitura_base_landing(ano, mes, data_interval_end)

    df_landing["ano_particao"] = ano
    df_landing["mes_particao"] = mes
    df_landing["timestamp_dagrun"] = data_interval_end
    df_landing["timestamp_escrita"] = datetime.now()

    df_landing = apply_schema(
        df=df_landing, schema=SCHEMA_INFORME_FUNDOS, rename_cols=True, select_cols=True
    )

    path = "/root/pessoal/FundosRAG-CVM/airflow-dags/dags/utils/dados_informe_diario/fundos/bronze/"
    write_deltalake(
        path,
        df_landing,
        partition_by=["ano_particao", "mes_particao"],
        mode="append",
    )
    dt = DeltaTable(path)

    dt.optimize.compact()
    dt.vacuum()
    dt.create_checkpoint()
    dt.cleanup_metadata()
