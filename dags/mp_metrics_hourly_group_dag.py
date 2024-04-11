import os
import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping, SparkThriftProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SparkThriftProfileMapping(
        conn_id="spark_default",
        profile_args={"schema": "dbt_test"},
    ),
)

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "task_group"],
)
def mp_metrics_dbt_task_group() -> None:
    """
    This is an example of a simple DAG.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")
    
    mp_metrics_hourly = DbtTaskGroup(
        group_id="mp_metrics_project",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dags/dbt/mp_metrics_hourly",
            dbt_vars={"execution_date_pdt": "{{ ds }}"}
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        )
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> mp_metrics_hourly >> post_dbt

mp_metrics_dbt_task_group()