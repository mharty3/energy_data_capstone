# Batch Model Deployment with Airflow

To deploy the model that was trained and logged to the MLFlow model registry, I used the same Airflow instance that is orchestrating the data collection pipeline. 

The batch_predict script in this directory was modified into an [Airflow DAG](../02_airflow/dags/batch_predict_dag.py). Every night at midnight, the DAG pulls the model from MLFlow, pulls the necessary data from the data warehouse, runs the model, and loads the next set of predictions into the Data Warehouse.

