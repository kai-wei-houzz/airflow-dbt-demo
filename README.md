## Local Airflow Using Astro
`brew install astro`
`astro dev init`
`astro dev start`

- Modify docker file
```
# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir <your-dbt-adapter> && deactivate
```

## Reference
- Astro CLI: https://docs.astronomer.io/astro/cli/get-started-cli