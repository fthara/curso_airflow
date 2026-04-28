import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="Pools",
    description="Exemplo de Pools",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso", "exemplo"]
) as dag:
    
    task_leve = BashOperator(
        task_id="task_leve",
        bash_command="sleep 5",
        pool="meupool",
        priority_weight=1,
        weight_rule="absolute"
    )

    task_medio = BashOperator(
        task_id="task_medio",
        bash_command="sleep 5",
        pool="meupool",
        priority_weight=5,
        weight_rule="absolute"
    )

    task_pesado = BashOperator(
        task_id="task_pesado",
        bash_command="sleep 5",
        pool="meupool",
        pool_slots=2,
        priority_weight=10,
        weight_rule="absolute"
    )
