# Cloud-Based Apache Airflow DAG for Data Cleaning

## Contributors 
- Edgar Valero [@edgarvalero23](https://github.com/edgarvalero23)
- Matt Doyle [@pirateking92](https://github.com/pirateking92)
- Oli Kelly [@olikelly00](https://github.com/olikelly00)

## Project Overview
This project demonstrates how to build, test, and deploy an Apache Airflow DAG in a cloud environment (AWS EC2). The DAG automates data cleaning processes, particularly for post content, using PostgreSQL and Pandas. This ensures structured and high-quality data for downstream analytics.

## Technical Highlights
- **Apache Airflow:** Used to orchestrate and schedule data cleaning tasks.
- **PostgreSQL Integration:** Queries executed using `psycopg2` and `SQLExecuteQueryOperator`.
- **AWS EC2 Deployment:** Configured Airflow on an EC2 instance with PostgreSQL.
- **Parallelism :** Set up 10 concurrent processes to handle data transfer 10x more efficiently.
- **Python & Pandas:** Used for data transformation and cleaning.

## DAG Workflow
- Extracts raw post data from the database.
- Cleans and normalises text fields.
- Removes duplicates and invalid entries.
- Updates the cleaned data back into PostgreSQL.

## Challenges & How we Overcame Them
One major challenge was designing a data pipeline that could efficiently handle an immense volume of data. Our source dataset contained 57 million rows, and our initial pipeline design would have taken 26 hours to process all the batches. To optimize performance, we implemented parallel processing, running 10 concurrent processes. This has made our data pipeline 10x faster and more efficient.   

## Future Improvements
- Implement monitoring and alerting mechanisms to detect DAG failures and automate retries.
- Optimise data cleaning processes for better performance at scale.
- Enhance logging and error-handling for better observability.

## Key Learnings
- Setting up and running Airflow DAGs in a cloud environment.
- Managing environment dependencies for DAG execution.
- Using Airflow’s SQL and Python operators to interact with databases.
- Debugging DAG failures using Airflow’s UI and logs.
- Deploying and troubleshooting DAGs on an EC2 instance.
- Best practices for structuring DAGs for modularity and reusability.



