from airflow import DAG
import airflow
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "sparkflow",
    default_args = {
        "owner": "Vadym Urupa",
        "start_date": airflow.urils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id = "start",
    python_callable = lambda: print("Job started. ")
    dag = dag
)

python_job = SparkSubmitOperator(
    task_id = "python_job",
    conn_id = "spark-conn",
    application = "jobs/python/wordcount.py",
    dag = dag
)

scala_job = SparkSubmitOperator(
    task_id = "scala_job",
    conn_id = "spark-conn",
    application = "jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar",
    dag = dag
)

java_job = SparkSubmitOperator(
    task_id = "java_job",
    conn_id = "spark-conn",
    application = "jobs/java/spark-job/target/spark-job-1.0-SNAPSHOT.jar",
    java_class = "com.airscolar.spark.WordCountJob",
    dag = dag
)

end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("Job completed successfully. ")
    dag = dag
)

start >> [python_job, scala_job, java_job] >> end
