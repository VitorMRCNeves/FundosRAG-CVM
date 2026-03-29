"""
# DAG Lâmina FI

## Descrição
Orquestra a extração das lâminas de fundos de investimento da CVM e o processamento nas camadas
Landing → Bronze → Silver para as quatro tabelas: lamina_fi, carteira, rentab_ano e rentab_mes.

## Fluxo de Processamento
1. **Extrai Lâmina FI (Landing)**: Baixa o ZIP mensal da CVM, extrai os 4 CSVs e salva na Landing
   particionada por ano/mês. Faz skip gracioso se o arquivo ainda não foi publicado (HTTP 404).
2. **Landing → Bronze** (4 caminhos paralelos): Tipagem para string, enriquecimento de metadados
   e escrita Delta particionada (append).
3. **Bronze → Silver** (4 caminhos paralelos): Seleção do snapshot mais recente
   (max timestamp_dagrun → max timestamp_escrita) e overwrite particionado.

## Fontes de Dados
- **CVM LAMINA_FI**: `https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/`

## Schedule
- **Frequência**: Diária
- **Horário**: 10:00
- **Dias**: Dias úteis (seg-sex)

## Comportamento Especial
- **Idempotência**: Sim (Silver com overwrite de partição; Bronze em append com deduplicação por timestamp)
- **Skip Condition**: HTTP 404 na CVM → AirflowSkipException propagada a todos os downstream tasks
- **Controle de Concorrência**: max_active_runs=1
"""

from airflow.decorators import dag, task

timestamp_dagrun = "{{ dag_run.conf.get('timestamp_dagrun', data_interval_end) }}"

default_args = {
    "owner": "Vitor Neves",
    "start_date": "2025-03-28",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "params": {},
}


@dag(
    dag_id="DagLaminaFI",
    default_args=default_args,
    schedule_interval="0 10 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    render_template_as_native_obj=True,
    description="Extrai e processa Lâminas FI da CVM (Landing → Bronze → Silver)",
    doc_md=__doc__,
)
def dag_lamina_fi():
    """
    DAG que processa as Lâminas de Fundos de Investimento conforme arquitetura Medalhão.
    """

    @task
    def task_extrai_lamina_fi(timestamp_dagrun: str) -> None:
        """
        Baixa o ZIP mensal de lâminas da CVM, extrai os 4 CSVs e salva na Landing.
        Lança AirflowSkipException se o arquivo ainda não estiver disponível (HTTP 404).

        Args:
            timestamp_dagrun: Data/hora de referência (ISO) para determinar ano/mês de extração.
        """
        from utils.DagLaminaFI.task_extrai_lamina_fi.task_extrai_lamina_fi import (
            fn_extrai_lamina_fi,
        )

        fn_extrai_lamina_fi(timestamp_dagrun)

    @task
    def task_transferencia_landing_bronze_lamina(timestamp_dagrun: str) -> None:
        """
        Processa tabela lamina_fi: Landing → Bronze aplicando schema e metadados.

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_landing_bronze_lamina.task_transferencia_landing_bronze_lamina import (
            fn_transferencia_landing_bronze_lamina,
        )

        fn_transferencia_landing_bronze_lamina(timestamp_dagrun)

    @task
    def task_transferencia_bronze_silver_lamina(timestamp_dagrun: str) -> None:
        """
        Publica snapshot lamina_fi Bronze → Silver (overwrite particionado).

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_bronze_silver_lamina.task_transferencia_bronze_silver_lamina import (
            fn_transferencia_bronze_silver_lamina,
        )

        fn_transferencia_bronze_silver_lamina(timestamp_dagrun)

    @task
    def task_transferencia_landing_bronze_carteira(timestamp_dagrun: str) -> None:
        """
        Processa tabela carteira: Landing → Bronze aplicando schema e metadados.

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_landing_bronze_carteira.task_transferencia_landing_bronze_carteira import (
            fn_transferencia_landing_bronze_carteira,
        )

        fn_transferencia_landing_bronze_carteira(timestamp_dagrun)

    @task
    def task_transferencia_bronze_silver_carteira(timestamp_dagrun: str) -> None:
        """
        Publica snapshot carteira Bronze → Silver (overwrite particionado).

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_bronze_silver_carteira.task_transferencia_bronze_silver_carteira import (
            fn_transferencia_bronze_silver_carteira,
        )

        fn_transferencia_bronze_silver_carteira(timestamp_dagrun)

    @task
    def task_transferencia_landing_bronze_rentab_ano(timestamp_dagrun: str) -> None:
        """
        Processa tabela rentab_ano: Landing → Bronze aplicando schema e metadados.

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_landing_bronze_rentab_ano.task_transferencia_landing_bronze_rentab_ano import (
            fn_transferencia_landing_bronze_rentab_ano,
        )

        fn_transferencia_landing_bronze_rentab_ano(timestamp_dagrun)

    @task
    def task_transferencia_bronze_silver_rentab_ano(timestamp_dagrun: str) -> None:
        """
        Publica snapshot rentab_ano Bronze → Silver (overwrite particionado).

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_bronze_silver_rentab_ano.task_transferencia_bronze_silver_rentab_ano import (
            fn_transferencia_bronze_silver_rentab_ano,
        )

        fn_transferencia_bronze_silver_rentab_ano(timestamp_dagrun)

    @task
    def task_transferencia_landing_bronze_rentab_mes(timestamp_dagrun: str) -> None:
        """
        Processa tabela rentab_mes: Landing → Bronze aplicando schema e metadados.

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_landing_bronze_rentab_mes.task_transferencia_landing_bronze_rentab_mes import (
            fn_transferencia_landing_bronze_rentab_mes,
        )

        fn_transferencia_landing_bronze_rentab_mes(timestamp_dagrun)

    @task
    def task_transferencia_bronze_silver_rentab_mes(timestamp_dagrun: str) -> None:
        """
        Publica snapshot rentab_mes Bronze → Silver (overwrite particionado).

        Args:
            timestamp_dagrun: Data/hora de referência (ISO).
        """
        from utils.DagLaminaFI.task_transferencia_bronze_silver_rentab_mes.task_transferencia_bronze_silver_rentab_mes import (
            fn_transferencia_bronze_silver_rentab_mes,
        )

        fn_transferencia_bronze_silver_rentab_mes(timestamp_dagrun)

    extrai = task_extrai_lamina_fi(timestamp_dagrun)

    lb_lamina = task_transferencia_landing_bronze_lamina(timestamp_dagrun)
    bs_lamina = task_transferencia_bronze_silver_lamina(timestamp_dagrun)

    lb_carteira = task_transferencia_landing_bronze_carteira(timestamp_dagrun)
    bs_carteira = task_transferencia_bronze_silver_carteira(timestamp_dagrun)

    lb_rentab_ano = task_transferencia_landing_bronze_rentab_ano(timestamp_dagrun)
    bs_rentab_ano = task_transferencia_bronze_silver_rentab_ano(timestamp_dagrun)

    lb_rentab_mes = task_transferencia_landing_bronze_rentab_mes(timestamp_dagrun)
    bs_rentab_mes = task_transferencia_bronze_silver_rentab_mes(timestamp_dagrun)

    extrai >> [lb_lamina, lb_carteira, lb_rentab_ano, lb_rentab_mes]
    lb_lamina >> bs_lamina
    lb_carteira >> bs_carteira
    lb_rentab_ano >> bs_rentab_ano
    lb_rentab_mes >> bs_rentab_mes


dag_lamina_fi = dag_lamina_fi()
