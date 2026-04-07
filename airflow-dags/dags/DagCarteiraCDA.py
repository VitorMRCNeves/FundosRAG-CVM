"""
# DAG Carteira CDA — Composição de Carteiras de Fundos (CVM)

## Descrição
Orquestra a extração mensal das carteiras de fundos de investimento da CVM (arquivo CDA),
processamento em Landing → Bronze (8 blocos paralelos) → Silver unificada.

## Fluxo de Processamento
1. **Extrai CDA (Landing)**: Baixa o ZIP mensal da CVM, extrai os 8 CSVs (BLC_1..BLC_8)
   e salva na Landing particionada por bloco/ano/mês. Lança AirflowSkipException se HTTP 404.
2. **Landing → Bronze** (8 tasks paralelas): Para cada BLC, lê o CSV da Landing,
   tipifica todas as colunas como string, adiciona metadados e persiste em Delta Lake (append).
3. **Bronze → Silver** (1 task): Lê os 8 BLCs da Bronze, deduplica por
   max(timestamp_dagrun) → max(timestamp_escrita), consolida em schema unificado
   e escreve Silver em overwrite por partição.

## Fontes de Dados
- **CVM CDA**: `https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{ano}{mes}.zip`
- Contém 8 arquivos: `cda_fi_BLC_1_{ano}{mes}.csv` até `cda_fi_BLC_8_{ano}{mes}.csv`

## Schedule
- **Frequência**: Diária
- **Horário**: 10:00
- **Dias**: Dias úteis (seg-sex)

## Comportamento Especial
- **Idempotência**: Sim — Silver com overwrite de partição; Bronze em append com deduplicação
- **Skip Condition**: HTTP 404 na CVM → AirflowSkipException propagada ao downstream
- **Backfill histórico**: Passe `{"timestamp_dagrun": "YYYY-MM-DDTHH:MM:SS"}` em dag_run.conf
- **Controle de Concorrência**: max_active_runs=1

## Silver Unificada
A tabela Silver `fundos/cda/` consolida os 8 BLCs em um único Delta Lake.
A coluna `bloco` (ex: `"BLC_1"`) identifica a origem de cada registro.
Colunas específicas de cada bloco são NaN para registros de outros blocos.
Use `dt_comptc` como data de referência para análises temporais.
"""

from airflow.decorators import dag, task
import pandas as pd

timestamp_dagrun = pd.to_datetime(
    "2026-03-31"
)  # "{{ (dag_run.conf.get('timestamp_dagrun', data_interval_end) | macros.ds_add(-30)) }}"

default_args = {
    "owner": "Vitor Neves",
    "start_date": "2025-01-01",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "params": {},
}


@dag(
    dag_id="DagCarteiraCDA",
    default_args=default_args,
    schedule_interval="0 10 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    render_template_as_native_obj=True,
    description="Extrai e processa carteiras CDA de Fundos da CVM (Landing → Bronze × 8 → Silver unificada)",
    doc_md=__doc__,
)
def dag_carteira_cda():
    """DAG que processa as carteiras CDA de Fundos conforme arquitetura Medalhão."""

    @task
    def task_extrai_carteira_cda(timestamp_dagrun: str) -> None:
        """
        Baixa o ZIP mensal de carteiras CDA, extrai 8 CSVs e salva na Landing.
        Lança AirflowSkipException se o arquivo ainda não estiver disponível (HTTP 404).
        """
        from utils.DagCarteiraCDA.task_extrai_carteira_cda.task_extrai_carteira_cda import (
            fn_extrai_carteira_cda,
        )

        fn_extrai_carteira_cda(timestamp_dagrun)

    @task
    def task_transferencia_landing_bronze_blc_1(timestamp_dagrun: str) -> None:
        """Processa BLC_1 (Títulos Públicos): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_1")

    @task
    def task_transferencia_landing_bronze_blc_2(timestamp_dagrun: str) -> None:
        """Processa BLC_2 (Cotas de Fundos): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_2")

    @task
    def task_transferencia_landing_bronze_blc_3(timestamp_dagrun: str) -> None:
        """Processa BLC_3 (SWAP): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_3")

    @task
    def task_transferencia_landing_bronze_blc_4(timestamp_dagrun: str) -> None:
        """Processa BLC_4 (Outros Ativos/Derivativos): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_4")

    @task
    def task_transferencia_landing_bronze_blc_5(timestamp_dagrun: str) -> None:
        """Processa BLC_5 (RF Privada com Agência de Risco): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_5")

    @task
    def task_transferencia_landing_bronze_blc_6(timestamp_dagrun: str) -> None:
        """Processa BLC_6 (RF Privada sem Agência de Risco): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_6")

    @task
    def task_transferencia_landing_bronze_blc_7(timestamp_dagrun: str) -> None:
        """Processa BLC_7 (Ativos no Exterior): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_7")

    @task
    def task_transferencia_landing_bronze_blc_8(timestamp_dagrun: str) -> None:
        """Processa BLC_8 (Outros): Landing → Bronze."""
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )

        fn_processa_landing_bronze_blc(timestamp_dagrun, "blc_8")

    @task(trigger_rule="all_done")
    def task_transferencia_bronze_silver(timestamp_dagrun: str) -> None:
        """
        Consolida os 8 BLCs da Bronze em única tabela Silver (overwrite por partição).
        Deduplicação por max(timestamp_dagrun) → max(timestamp_escrita).
        """
        from utils.DagCarteiraCDA.task_transferencia_bronze_silver.task_transferencia_bronze_silver import (
            fn_transferencia_bronze_silver,
        )

        fn_transferencia_bronze_silver(timestamp_dagrun)

    extrai = task_extrai_carteira_cda(timestamp_dagrun)

    lb_blc_1 = task_transferencia_landing_bronze_blc_1(timestamp_dagrun)
    lb_blc_2 = task_transferencia_landing_bronze_blc_2(timestamp_dagrun)
    lb_blc_3 = task_transferencia_landing_bronze_blc_3(timestamp_dagrun)
    lb_blc_4 = task_transferencia_landing_bronze_blc_4(timestamp_dagrun)
    lb_blc_5 = task_transferencia_landing_bronze_blc_5(timestamp_dagrun)
    lb_blc_6 = task_transferencia_landing_bronze_blc_6(timestamp_dagrun)
    lb_blc_7 = task_transferencia_landing_bronze_blc_7(timestamp_dagrun)
    lb_blc_8 = task_transferencia_landing_bronze_blc_8(timestamp_dagrun)

    silver = task_transferencia_bronze_silver(timestamp_dagrun)

    extrai >> [
        lb_blc_1,
        lb_blc_2,
        lb_blc_3,
        lb_blc_4,
        lb_blc_5,
        lb_blc_6,
        lb_blc_7,
        lb_blc_8,
    ]
    [
        lb_blc_1,
        lb_blc_2,
        lb_blc_3,
        lb_blc_4,
        lb_blc_5,
        lb_blc_6,
        lb_blc_7,
        lb_blc_8,
    ] >> silver


dag_carteira_cda = dag_carteira_cda()
