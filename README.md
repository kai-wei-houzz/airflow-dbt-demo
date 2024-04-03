# Airflow dbt Pipeline 
## Desciption
This is a demo project of using dbt to build a data pipeline and schedule with Airflow. 
For the DBT project, please refer to here[].
Steps include:
1. Start a local Airflow with Astro
2. Parse an existing dbt project into an Airflow DAG with cosmos

## Local Airflow Using Astro
- Install Astro packages and initialize a new project
```bash
# We are all using Mac right?
brew install astro

# This will initialize the project with a basic structure
astro dev init
```

- Modify `Dockerfile`. We need 2 adapters in this example ([reference](https://docs.getdbt.com/docs/core/connect-data-platform/spark-setup)):
    - `dbt-spark`: dbt core package and spark adapter
    - `dbt-spark[PyHive]`: For connecting to Spark with Thrift
```yaml
# install dbt into a virtual environment
# We need dbt-spark and 
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir <your-dbt-adapter> && deactivate
```

- Add required packages in `requirements.txt`
```text
apache-airflow-providers-apache-spark
astronomer-cosmos
```

- Start the dev 
`astro dev start`

- Add reuqired connections

- Stop the Airflow
```
astro dev stop
```

## Note
- Can always add config to overwrite the default in `dbt_project`
```yaml
# To materialze model as a table
{{ config(materialized='table') }}
```
## Feedback
### Pros
- PRETTY COOL
- Users can manage their own dependencies
- Centralize configuration, such as connections, storage type. 
    - can be defined by subfolders
    - can be overwritten inside files
    - Don't need extra DDL and can control table schema with yml files
- Data tests included
- Able to integrate with existing operators
- DS can focus on SQL

### Cons
- More tech stack.
- Require Airflow version 2.5 or above. 


## Reference
- [Astro CLI](https://docs.astronomer.io/astro/cli/get-started-cli)
- [Cosmos](https://astronomer.github.io/astronomer-cosmos/getting_started/open-source.html)
- [DBT Spark Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/spark-setup)
- [Dag testing](https://docs.astronomer.io/learn/testing-airflow)