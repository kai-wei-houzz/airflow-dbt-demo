## Local Airflow Using Astro
`brew install astro`
`astro dev init`

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


## Reference
- Astro CLI: https://docs.astronomer.io/astro/cli/get-started-cli