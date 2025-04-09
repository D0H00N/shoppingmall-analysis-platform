from pyspark.sql import SparkSession
import datetime
import os
import pymysql
import subprocess
import traceback

MYSQL_JAR_PATH = "/usr/local/lib/mysql-connector-j-8.0.33.jar"

spark = SparkSession.builder \
    .appName("ProcessReviews") \
    .config("spark.jars", MYSQL_JAR_PATH) \
    .config("spark.driver.extraClassPath", MYSQL_JAR_PATH) \
    .config("spark.executor.extraClassPath", MYSQL_JAR_PATH) \
    .getOrCreate()

TODAY = datetime.datetime.today().strftime("%Y%m%d")
HDFS_REVIEWS_DIR = f"hdfs://15.152.242.221:9000/user/hadoop/musinsa/{TODAY}/"

df = spark.read.parquet(os.path.join(HDFS_REVIEWS_DIR, "*.parquet"))
df = df.dropDuplicates(["review_id"])

DB_HOST = "15.152.242.221"
DB_PORT = 3306
DB_USER = "admin"
DB_PASSWORD = "Admin@1234"
DB_NAME = "musinsa_pd_rv"
DB_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?useSSL=false&allowPublicKeyRetrieval=true"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

if df.count() > 0:
    try:
        print(f"[INFO] MySQL 적재 중, {df.count()}개의 리뷰를 저장")

        # MySQL 연결
        conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()

        # 기존 temp_reviews 테이블 초기화
        cursor.execute("TRUNCATE TABLE temp_reviews;")
        conn.commit()
        print("[INFO] temp_reviews 테이블 초기화 완료")

        # temp_reviews 테이블에 새로운 데이터 적재
        df.write.jdbc(url=DB_URL, table="temp_reviews", mode="append", properties=DB_PROPERTIES)
        print("[INFO] temp_reviews 테이블에 데이터 추가 완료")

        # 기존 데이터 개수 확인
        cursor.execute("SELECT COUNT(*) FROM reviews;")
        before_insert_count = cursor.fetchone()[0]
        print(f"[INFO] 적재 전 reviews 테이블 데이터 개수: {before_insert_count}")

        # 중복 리뷰 확인 및 처리
        cursor.execute("""
            SELECT COUNT(*) FROM temp_reviews 
            INNER JOIN reviews ON temp_reviews.review_id = reviews.review_id;
        """)
        already_exists_count = cursor.fetchone()[0]
        print(f"[INFO] 이미 존재하는 리뷰 개수: {already_exists_count}")

        cursor.execute("""
            SELECT COUNT(*) FROM temp_reviews 
            INNER JOIN reviews 
            ON temp_reviews.review_id = reviews.review_id
            WHERE temp_reviews.rating = reviews.rating 
            AND temp_reviews.review_text = reviews.review_text 
            AND temp_reviews.review_size = reviews.review_size;
        """)
        unchanged_count = cursor.fetchone()[0]
        print(f"[INFO] 기존 데이터와 동일하여 업데이트할 필요 없는 리뷰 개수: {unchanged_count}")

        # 새로운 리뷰 삽입
        insert_new_query = """
            INSERT IGNORE INTO reviews (review_id, product_id, rating, review_text, review_date, review_size)
            SELECT review_id, product_id, rating, review_text, CURDATE(), review_size FROM temp_reviews;
        """
        cursor.execute(insert_new_query)
        conn.commit()
        print("[INFO] reviews 테이블에 새로운 데이터 삽입 완료")

        # 기존 리뷰 업데이트
        update_existing_query = """
            INSERT INTO reviews (review_id, product_id, rating, review_text, review_date, review_size)
            SELECT review_id, product_id, rating, review_text, CURDATE(), review_size FROM temp_reviews
            ON DUPLICATE KEY UPDATE 
                rating = VALUES(rating), 
                review_text = VALUES(review_text), 
                review_date = VALUES(review_date), 
                review_size = VALUES(review_size);
        """
        cursor.execute(update_existing_query)
        conn.commit()
        print("[INFO] reviews 테이블 기존 데이터 업데이트 완료")

        # 적재 후 데이터 개수 확인
        cursor.execute("SELECT COUNT(*) FROM reviews;")
        after_insert_count = cursor.fetchone()[0]
        print(f"[INFO] 적재 후 reviews 테이블 데이터 개수: {after_insert_count}")

        # 삽입된 데이터 개수 출력
        inserted_count = after_insert_count - before_insert_count
        if inserted_count == 0:
            print("[INFO] MySQL에 0개의 리뷰가 추가됨")
            if already_exists_count == df.count():
                print("[DEBUG] 모든 리뷰가 이미 reviews 테이블에 존재함. 중복 삽입 방지됨.")
            elif unchanged_count == df.count():
                print("[DEBUG] 모든 리뷰가 기존 데이터와 동일하여 업데이트할 필요 없음.")
            else:
                print("[DEBUG] 일부 데이터는 변경이 필요하지만 업데이트 로직에서 제외됨.")
        else:
            print(f"[INFO] MySQL에 {inserted_count}개의 리뷰가 추가됨")

        cursor.close()
        conn.close()

        # 감성 분석 실행
        print("[INFO] 감성 분석을 시작합니다.")
        result = subprocess.run(
            ["python3", "/home/lab01/airflow/crawling_process/review_sentiment.py"],
            check=True,
            capture_output=True,
            text=True
        )
        print("[INFO] 감성 분석 실행 완료.")
        print(result.stdout)

    except Exception as e:
        print(f"[ERROR] MySQL 적재 실패: {e}")
        print(traceback.format_exc())

spark.stop()
