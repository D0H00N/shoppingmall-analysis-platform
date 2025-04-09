from airflow import DAG
from airflow.exceptions import AirflowTaskTimeout
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# 크롤링 및 데이터 처리 스크립트 경로
CRAWLING_SCRIPT = "/home/lab01/airflow/crawling_process/crawling.py"
PROCESS_SCRIPT = "/home/lab01/airflow/crawling_process/process_pyspark.py"
SENTIMENT_SCRIPT = "/home/lab01/airflow/crawling_process/review_sentiment.py"

# MySQL Connector JAR 경로 (버전 일치 확인)
MYSQL_JAR_PATH = "/usr/local/lib/mysql-connector-j-8.0.33.jar"

# Python 환경 변수 설정
PYTHON_ENV = "/home/ubuntu/anaconda3/envs/airflow_env/bin/python3"

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
        "export DISPLAY=:99 && "
        f"{PYTHON_ENV} {CRAWLING_SCRIPT} >> /home/lab01/airflow/logs/crawling.log 2>&1"
    ),
    env={
        "PATH": "/usr/bin:" + os.environ["PATH"],
        "PYTHONPATH": "/home/ubuntu/anaconda3/envs/airflow_env/lib/python3.8/site-packages",
    },
    execution_timeout=timedelta(hours=2),  # 실행 제한 시간을 2시간으로 늘림
    retries=3,  # 실패 시 3번 재시도
    retry_delay=timedelta(minutes=10),  # 10분 후 재시도
    dag=dag,
)

# PySpark를 활용한 HDFS → MySQL 적재 실행 (5개 배치마다 실행)
task_process_reviews = BashOperator(
    task_id="process_reviews",
    bash_command=(
        f"spark-submit --jars {MYSQL_JAR_PATH} {PROCESS_SCRIPT} >> /home/lab01/airflow/logs/process_pyspark.log 2>&1 && "
        f"echo '[INFO] MySQL 적재 완료'"
    ),
    dag=dag,
)

# 감성 분석 실행
task_sentiment_analysis = BashOperator(
    task_id="sentiment_analysis",
    bash_command=(
        f"{PYTHON_ENV} {SENTIMENT_SCRIPT} >> /home/lab01/airflow/logs/sentiment_analysis.log 2>&1"
    ),
    dag=dag,
)

# 5개 배치 크롤링 후 데이터 처리 실행 → 감성 분석 실행
task_crawl_reviews >> task_process_reviews >> task_sentiment_analysis
