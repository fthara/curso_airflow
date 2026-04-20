import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email

def on_fail(ctx):
    send_email(
        to=["fernandohara91@gmail.com"],
        subject=f"[FAIL] {ctx['ti'].task_id}",
        html_content="Falhou"
    )

def on_ok(ctx):
        send_email(
        to=["fernandohara91@gmail.com"],
        subject=f"[OK] {ctx['ti'].task_id}",
        html_content="Sucesso"
    )

default_args = {
    "depends_on_past": False,
    "email": ["fernandohara91@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}

with DAG(
    dag_id="EmailCallback",
    description="DAG teste email on callback",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=default_args,
    tags=["curso", "exemplo", "email"]
) as dag:
    task1 = BashOperator(task_id="tsk1", bash_command="exit 1", on_failure_callback=on_fail)

    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", on_success_callback=on_ok,
                         trigger_rule=TriggerRule.ALL_DONE)
    
    task1 >> task2