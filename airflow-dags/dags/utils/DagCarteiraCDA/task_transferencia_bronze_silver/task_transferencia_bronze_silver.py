import os
from dateutil.parser import isoparse
from datetime import datetime
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from utils.DagCarteiraCDA.task_transferencia_bronze_silver.sh_schema_silver import (
    BLC_TO_SILVER_MAP,
    SILVER_COLS_ORDER,
    SH_SCHEMA_SILVER,
)
from utils.utilitarios import apply_schema

_BLOCOS = [f"blc_{i}" for i in range(1, 9)]

_DATE_COLS = [
    "dt_comptc",
    "dt_confid_aplic",
    "dt_emissao",
    "dt_venc",
    "dt_ini_vigencia",
    "dt_fim_vigencia",
    "dt_risco",
    "timestamp_dagrun",
    "timestamp_escrita",
]
_FLOAT_COLS = [
    "pr_cupom_posfx",
    "pr_indexador_posfx",
    "pr_taxa_prefx",
    "qt_ativo_exterior",
    "vl_ativo_exterior",
    "qt_aquis_negoc",
    "qt_pos_final",
    "qt_venda_negoc",
    "vl_aquis_negoc",
    "vl_custo_pos_final",
    "vl_merc_pos_final",
    "vl_venda_negoc",
]


def _fn_normaliza_bloco(df: pd.DataFrame, bloco: str) -> pd.DataFrame:
    """
    Filtra max(timestamp_dagrun) → max(timestamp_escrita), renomeia colunas para Silver,
    adiciona coluna 'bloco' e alinha ao schema Silver completo (colunas ausentes = NaN).
    """
    df_max_run = df.loc[df["timestamp_dagrun"] == df["timestamp_dagrun"].max()]
    df_final = df_max_run.loc[
        df_max_run["timestamp_escrita"] == df_max_run["timestamp_escrita"].max()
    ]

    df_final = df_final.rename(columns=BLC_TO_SILVER_MAP[bloco])
    df_final = df_final.copy()
    df_final["bloco"] = bloco.upper()

    return df_final.reindex(columns=SILVER_COLS_ORDER)


def fn_normaliza_posicoes(df: pd.DataFrame):
    """
    Normaliza e padroniza o DataFrame resultante da consolidação dos blocos financeiros, atribuindo valores padronizados para as colunas 'codigo', 'emissor' e 'indexador' conforme o tipo de bloco. Este processo garante que os principais atributos estejam corretamente alinhados e preenchidos para cada tipo de ativo, alinhando ao schema Silver e preparando o DataFrame para etapas subsequentes de tratamento e análise.
    """

    df["indexador"] = df["ds_indexador_posfx"].map(
        {
            "DI de um dia": "POS",
            "Taxa de juro prefixada": "PRÉ",
            "Índice de Preços ao Consumidor Amplo (IPCA/IBGE)": "IPCA",
            "Taxa Selic": "SELIC",
        }
    )

    df.loc[df["bloco"] == "BLC_1", "codigo"] = df["cd_isin"]
    df.loc[df["bloco"] == "BLC_1", "emissor"] = df["tp_titulo_pub"]

    df.loc[df["bloco"] == "BLC_2", "codigo"] = df["cnpj_fundo_cota"]
    df.loc[df["bloco"] == "BLC_2", "emissor"] = df["nm_fundo_cota"]

    df.loc[df["bloco"] == "BLC_3", "codigo"] = df["cd_swap"]
    df.loc[df["bloco"] == "BLC_3", "indexador"] = df["ds_swap"]

    df.loc[df["bloco"] == "BLC_4", "codigo"] = df["cd_ativo"]
    df.loc[df["bloco"] == "BLC_4", "emissor"] = df["ds_ativo"]

    df.loc[df["bloco"] == "BLC_4", "codigo"] = df["cpf_cnpj_emissor"]
    df.loc[df["bloco"] == "BLC_4", "emissor"] = df["nm_emissor"]

    df.loc[df["bloco"] == "BLC_5", "codigo"] = df["cpf_cnpj_emissor"]
    df.loc[df["bloco"] == "BLC_5", "emissor"] = df["nm_emissor"]

    df.loc[df["bloco"] == "BLC_6", "codigo"] = df["cpf_cnpj_emissor"]
    df.loc[df["bloco"] == "BLC_6", "emissor"] = df["nm_emissor"]

    df.loc[df["bloco"] == "BLC_7", "codigo"] = df["cd_ativo_bv_merc"]
    df.loc[df["bloco"] == "BLC_7", "emissor"] = df["nm_emissor"]

    df.loc[df["bloco"] == "BLC_8", "codigo"] = df["cpf_cnpj_emissor"]
    df.loc[df["bloco"] == "BLC_8", "emissor"] = df["ds_ativo"]

    df.loc[
        :,
        [
            "cnpj_fundo",
            "denom_social",
            "tp_fundo_classe",
            "dt_comptc",
            "tp_ativo",
            "codigo",
            "emissor",
            "indexador",
            "qt_pos_final",
            "vl_merc_pos_final",
            "ano_particao",
            "mes_particao",
            "timestamp_dagrun",
            "timestamp_escrita",
        ],
    ]
    return df


def fn_transferencia_bronze_silver(data_interval_end: str) -> None:
    """
    Consolida os 8 BLCs de Bronze em uma única tabela Silver.
    Para cada BLC: filtra max timestamp_dagrun → max timestamp_escrita,
    renomeia colunas e alinha ao schema unificado. Escreve Silver em overwrite por partição.

    Args:
        data_interval_end: Data/hora de referência ISO para determinar a partição.
    """
    bucket_bronze = os.environ["BUCKET_BRONZE"]
    bucket_silver = os.environ["BUCKET_SILVER"]

    if isinstance(data_interval_end, str):
        data_interval_end = isoparse(data_interval_end)

    ano = data_interval_end.year
    mes = data_interval_end.month

    frames = []
    for bloco in _BLOCOS:
        path = f"s3://{bucket_bronze}/fundos/cda/{bloco}"
        try:
            df = DeltaTable(path).to_pandas(
                filters=[("ano_particao", "=", ano), ("mes_particao", "=", mes)]
            )
        except TableNotFoundError:
            continue

        frames.append(_fn_normaliza_bloco(df, bloco))

    df_silver = pd.concat(frames, ignore_index=True)
    df_silver = fn_normaliza_posicoes(df=df_silver)

    df_silver["timestamp_escrita"] = datetime.now()

    df_silver = apply_schema(df_silver, SH_SCHEMA_SILVER)

    path = f"s3://{bucket_silver}/fundos/cda/"
    write_deltalake(
        path,
        df_silver,
        partition_by=["ano_particao", "mes_particao"],
        mode="overwrite",
    )
    dt = DeltaTable(path)
    dt.optimize.compact()
    dt.vacuum()
    dt.create_checkpoint()
    dt.cleanup_metadata()
