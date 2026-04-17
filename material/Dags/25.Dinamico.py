import pendulum
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta

ITENS = ['sp','rj','bh','rs']

with DAG(
    dag_id="dinamico",
    description="dinamico",
    schedule=timedelta(minutes=1),
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:


    @task
    def baixar(nome: str) -> str:
        print(f"Baixando {nome}...")
        return nome
    
    @task
    def processar(nome: str) -> str:
        print(f"Processando {nome}...")
        return f"ok: {nome}"
    
    @task
    def consolidar(resultados: list[str]) -> str:
        print("Consolidado: ", resultados)


    baixados = baixar.expand(nome=ITENS)    
    processados = processar.expand(nome=baixados)
    consolidar(processados)
