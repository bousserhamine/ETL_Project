from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_arg = {
    'owner':'airflow',
    'retries' : 5 ,
    'retry_delay' : timedelta(minutes=2)
}

def greent(name , age):
    print(f"Here We Go , I am {name} and i am {age}.")

with DAG(
    dag_id= "pyop_test3",
    default_args=default_arg,
    description= "This is my py Op dag ",
    start_date= datetime(2024, 4 , 11 , 12) ,
    schedule_interval='@daily',
    tags=["test2"]
) as dag :
    task1=PythonOperator(
        task_id= 'greent',
        python_callable= greent,
        op_kwargs={'name':' Amine','age': 24}
    )

    task1