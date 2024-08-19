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
        'pyspark_movie',
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
    end_date=datetime(2017, 2, 1),
    #end_date=datetime(2017, 2, 1),
    catchup=True,
    tags=['pyspark', 'movie'],
) as dag:

    
    def re_partition(ds_nodash):
        from spark_flow.re import re_partition
        re_partition(ds_nodash)

    def branch_op(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = f"{home_dir}/data/movie/repartition/load_dt={ds_nodash}"

        if os.path.exists(path):
            return rm_dir.task_id
        else:
            return re_partition.task_id
        
        print("branch fin!")

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/data/movie/repartition/load_dt={{ds_nodash}}'
            )

    re_partition = PythonVirtualenvOperator(
        task_id='re.partition',
        python_callable=re_partition,
        requirements=['git+https://github.com/oddsummer56/spark_flow.git@0.1.0/simple'],
        system_site_packages=False,
        #trigger_rule="all_done",
        #venv_cache_path="/home/kim1/tmp2/airflow_venv/get_data"
    )
    
    join_df = BashOperator(
            task_id='join.df',
            bash_command="""
                SPARK_HOME=~/app/spark-3.5.1-bin-hadoop3
                AIRFLOW_HOME=~/airflow_pyspark
                $SPARK_HOME/bin/spark-submit $AIRFLOW_HOME/py/movie_join_df.py {{ds_nodash}}
                """
    )

    agg_df = BashOperator(
            task_id='agg.df',
            bash_command="""
                SPARK_HOME=~/app/spark-3.5.1-bin-hadoop3
                AIRFLOW_HOME=~/airflow_pyspark
                $SPARK_HOME/bin/spark-submit $AIRFLOW_HOME/py/movie_agg_df.py 
                """
            )


    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    branch_op = BranchPythonOperator(task_id="branch.op", python_callable=branch_op)
    task_start >> branch_op >> [re_partition, rm_dir]
    rm_dir >> re_partition
    re_partition >> join_df >> agg_df >> task_end
