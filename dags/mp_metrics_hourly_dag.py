import os
import pendulum
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping, SparkThriftProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    # profile_mapping=PostgresUserPasswordProfileMapping(
    #     conn_id="airflow_db",
    #     profile_args={"schema": "public"},
    # ),
    profile_mapping=SparkThriftProfileMapping(
        conn_id="spark_default",
        profile_args={"schema": "dbt_test"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dags/dbt/mp_metrics_hourly",
        dbt_vars={"execution_date_pdt": "{{ ds }}"}
        # dbt_vars={"execution_date_pdt": "2024-03-30"}
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1),
    catchup=False,
    dag_id="mp_metrics_hourly_cosmos",
    default_args={"retries": 2},
)


# -- {{ 
# --     config(
# --         materialized='table',
# --         schema='dbt_prod'
# --     )
# -- }}