"""
# DAG Informe Diário de Fundos

## Descrição
Orquestra a extração do informe diário de fundos da CVM e o processamento nas camadas Landing → Bronze → Silver.

## Fluxo de Processamento
1. **Extrai Informe Diário (Landing)**: Baixa o ZIP mensal da CVM, extrai o CSV do mês de referência e salva na Landing particionada por ano/mês.
2. **Landing → Bronze**: Tipagem para string, enriquecimento de metadados e escrita Delta particionada.
3. **Bronze → Silver**: Seleção do snapshot mais recente do período e overwrite particionado.

## Fontes de Dados
- **CVM INF_DIARIO_FI**: `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/`

## Schedule
- **Frequência**: Diária
- **Horário**: 12:00 (UTC do scheduler)
- **Dias**: Todos os dias

## Comportamento Especial
- **Idempotência**: Sim (escrita particionada por período; Silver com overwrite de partição)
- **Skip Condition**: Não aplicável
- **Controle de Concorrência**: max_active_runs=1

## Regras Especiais
- O parâmetro `corrige_mes_anterior` deve ser `True` na execução de 12:00 do dia 10 de cada mês (considerando timezone do scheduler; ajuste interno para America/Sao_Paulo).
"""

from airflow.decorators import dag, task


timestamp_dagrun = "{{ data_interval_end }}"
corrige_mes_anterior = "{{ (data_interval_end.in_timezone('America/Sao_Paulo').day == 10) and (data_interval_end.in_timezone('America/Sao_Paulo').hour == 12) }}"

default_args = {
    "owner": "Vitor Neves",
    "start_date": "2025-03-28",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


@dag(
    dag_id="DagInformeDiarioFundos",
    default_args=default_args,
    schedule_interval="30 10,12 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    render_template_as_native_obj=True,
    description="Extrai e processa Informe Diário de Fundos da CVM (Landing → Bronze → Silver)",
    doc_md=__doc__,
)
def dag_informe_diario_fundos():
    """
    DAG que processa o Informe Diário de Fundos conforme arquitetura Medalhão.
    """

    @task
    def task_extrai_informe_diario(
        timestamp_dagrun: str, corrige_mes_anterior: bool
    ) -> None:
        """
        Extrai informe diário da CVM e grava na Landing particionada.

        Args:
            timestamp_dagrun: Data/hora de referência (ISO) para determinar ano/mês de extração.
            corrige_mes_anterior: Se True, ajusta referência para o mês imediatamente anterior.
        """
        from utils.DagInformeDiarioFundos.task_extrai_informe_diario.task_extrai_informe_diario import (
            fn_get_informes_diarios,
        )

        fn_get_informes_diarios(
            timestamp_dagrun, corrige_mes_anterior=corrige_mes_anterior
        )

    @task
    def task_transferencia_landing_bronze(
        timestamp_dagrun: str, corrige_mes_anterior: bool
    ) -> None:
        """
        Processa dados Landing → Bronze aplicando schema e metadados.

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
            corrige_mes_anterior: Se True, ajusta referência para o mês imediatamente anterior.
        """
        from utils.DagInformeDiarioFundos.task_transferencia_landing_bronze.task_transferencia_landing_bronze import (
            fn_processa_landing_bronze,
        )

        fn_processa_landing_bronze(
            timestamp_dagrun, corrige_mes_anterior=corrige_mes_anterior
        )

    @task
    def task_transferencia_bronze_silver(
        timestamp_dagrun: str, corrige_mes_anterior: bool
    ) -> None:
        """
        Publica snapshot Bronze → Silver (overwrite particionado) selecionando o registro mais recente.

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
            corrige_mes_anterior: Se True, ajusta referência para o mês imediatamente anterior.
        """
        from utils.DagInformeDiarioFundos.task_transferencia_bronze_silver.task_transferencia_bronze_silver import (
            fn_transferencia_bronze_silver,
        )

        fn_transferencia_bronze_silver(
            timestamp_dagrun, corrige_mes_anterior=corrige_mes_anterior
        )

    extrai = task_extrai_informe_diario(timestamp_dagrun, corrige_mes_anterior)
    bronze = task_transferencia_landing_bronze(timestamp_dagrun, corrige_mes_anterior)
    silver = task_transferencia_bronze_silver(timestamp_dagrun, corrige_mes_anterior)

    extrai >> bronze >> silver


dag_informe_diario_fundos = dag_informe_diario_fundos()
