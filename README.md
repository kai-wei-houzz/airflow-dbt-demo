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

- Modify docker file
```bash
# install dbt into a virtual environment
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

## Feedback
### Pros
- PRETTY COOL
- Users can manage their own dependencies
- Centrilize configuration, such as storage type. 
    - can be defined by subfolders
    - can be overwritten inside files
    - Don't need extra DDL and can control table schema with yml files
- Tests included

### Cons
- More tech stack
- Airflow version 2.5


## Reference
- Astro CLI: https://docs.astronomer.io/astro/cli/get-started-cli
- Cosmos: https://astronomer.github.io/astronomer-cosmos/getting_started/open-source.html
