Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- Compile a model
```bash
# assign vars and select specific model
dbt compile --vars 'execution_date_pdt: 2024-03-28' --select mp_metrics_hourly 
```
- dbt run
```bash
dbt run --vars 'execution_date_pdt: 2024-03-28' --select mp_metrics_hourly
```
- dbt test
```bash
dbt test
```
- Documentation
```bash
dbt docs generate    
dbt docs serve
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
