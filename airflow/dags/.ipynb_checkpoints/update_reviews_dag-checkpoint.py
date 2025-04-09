from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 크롤링 및 데이터 처리 스크립트 경로
CRAWLING_SCRIPT = "/home/lab01/airflow/crawling_process/crawling.py"
PROCESS_SCRIPT = "/home/lab01/airflow/crawling_process/process_pyspark.py"
MYSQL_JAR_PATH = "/usr/local/lib/mysql-connector-java-5.1.49-bin.jar"

# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "update_reviews_dag",
    default_args=default_args,
    description="배치 단위로 리뷰 크롤링 및 MySQL 적재",
    schedule_interval="0 * * * *",  # 매시간 실행
    catchup=False,
)

# 크롤링 및 HDFS 저장 (5개 배치 실행)
task_crawl_reviews = BashOperator(
    task_id="crawl_reviews",
    bash_command=(
        f"export PATH=$PATH:/usr/bin && "
        f"export PYTHONPATH=$PYTHONPATH:/home/ubuntu/anaconda3/envs/airflow_env/lib/python3.8/site-packages && "
        f"/home/ubuntu/anaconda3/envs/airflow_env/bin/python3 {CRAWLING_SCRIPT} --batch"
    ),
    dag=dag,
)

# PySpark를 활용한 HDFS → MySQL 적재 실행 (5개 배치마다 실행)
task_process_reviews = BashOperator(
    task_id="process_reviews",
    bash_command=f"spark-submit --jars {MYSQL_JAR_PATH} {PROCESS_SCRIPT} && echo '[INFO] MySQL 적재 완료'",
    dag=dag,
)

# 5개 배치 크롤링 후 데이터 처리 실행
task_crawl_reviews >> task_process_reviews
