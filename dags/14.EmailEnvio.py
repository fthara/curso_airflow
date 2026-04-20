import pendulum
from airflow import DAG
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="test_email",
    description="Envio de Email",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso", "exemplo"]
) as dag:
    EmailOperator(
        task_id="send_email",
        to=["fernandohara91@gmail.com"],
        subject="Teste de email no Airflow",
        html_content="<p>Este é um teste de envio de email no Airflow.</p>"
    )    
