from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


from airflow.operators.python import (
        PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
        )

with DAG(
        'movies-dynamic-json',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(seconds=3),
        'max_active_tasks': 3,
        'max_active_runs': 1,
        },

    description='pyspark',
    #schedule=timedelta(days=1),
    schedule="0 5 * * *",
    start_date=datetime(2017, 1, 1),
    end_date=datetime(2018, 1, 1),
    catchup=True,
    tags=['pyspark', 'movie', 'json', 'dynamic'],
) as dag:

    
#    def re_partition(ds_nodash):
#        from spark_flow.re import re_partition
#        re_partition(ds_nodash)

#    re_partition = PythonVirtualenvOperator(
#        task_id='re.partition',
#        python_callable=re_partition,
#        requirements=['git+https://github.com/oddsummer56/spark_flow.git@0.1.0/simple'],
#        system_site_packages=False,
#        #trigger_rule="all_done",
#        #venv_cache_path="/home/kim1/tmp2/airflow_venv/get_data"
#    )

    def get_data():
        print()

    def pars_parq():
        print()

    get_data = PythonVirtualenvOperator(
            task_id='get.data',
            python_callable=get_data,
            requirements=['git+https://github.com/oddsummer56/spark_flow.git@0.3.0/movies_dynamic_json'],
            system_site_packages=False
            )

    pars_parq = PythonVirtualenvOperator(
            task_id='parsing.parquet',
            python_callable=pars_parq,
            requirements=['git+https://github.com/oddsummer56/spark_flow.git@0.3.0/movies_dynamic_json'],
            system_site_packages=False
            )
    
    select_parq = BashOperator(
            task_id='select.parquet',
            bash_command="""
                """
    )


    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    
    task_start >> get_data >> pars_parq >> select_parq >> task_end
