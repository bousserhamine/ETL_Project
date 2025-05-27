from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_arg = {
    'owner':'airflow',
    'retries' : 5 ,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id= "Dag_task",
    default_args=default_arg,
    description= "This is my second dag ",
    start_date= datetime(2024, 4 , 9 , 15) ,
    schedule='@daily',
    tags=["test1"]
) as dag :
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world , this is my first BashOperator commande "   
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey , i am task2 . Here We Go"
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo hey , i am task3 i will be running in the same time as task2 . Here We Go"
    )

#task dependencies methode 1
task1.set_downstream(task2)
task1.set_downstream(task3)

#task dependencies methode 2
#task1 >> task2
#task1 >> task3

#task dependencies methode 3
#task1 >> [task2,task3]