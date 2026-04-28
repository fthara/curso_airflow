import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context
from airflow.providers.standard.operators.python import ShortCircuitOperator

with DAG(
    dag_id="shortcircuit",
    description="Teste de ShortCircuitOperator",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso", "exemplo"]
) as dag:
    
    def gera_qualidade() -> int:
        return random.randint(0, 100)
    
    gera_qualidade = PythonOperator(
        task_id="gera_qualidade",
        python_callable=gera_qualidade
    )

    def qualidade_suficiente() -> bool:
        ctx = get_current_context()
        qualidade = ctx["ti"].xcom_pull(task_ids="gera_qualidade")
        return int(qualidade) >= 70
    
    short_circuit = ShortCircuitOperator(
        task_id="short_circuit",
        python_callable=qualidade_suficiente
    )

    processa = BashOperator(
        task_id="processa",
        bash_command="echo 'Processando dados de qualidade suficiente'"
    )

    finaliza = BashOperator(
        task_id="finaliza",
        bash_command="echo 'Finalizando processo'"
    )

    gera_qualidade >> short_circuit >> processa >> finaliza