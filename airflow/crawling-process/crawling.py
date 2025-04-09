import os
import pymysql
import pandas as pd
import gc
import sys
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# MySQL 연결 정보
DB_CONFIG = {
    "host": "15.152.242.221",
    "user": "admin",
    "password": "Admin@1234",
    "database": "musinsa_pd_rv"
}

# 현재 날짜를 YYYYMMDD 형식으로 가져오기
CURRENT_DATE = datetime.today().strftime("%Y%m%d")

# HDFS 저장 경로 및 로컬 저장 경로 (날짜별 폴더 생성)
HDFS_REVIEWS_DIR = f"hdfs://15.152.242.221:9000/user/hadoop/musinsa/{CURRENT_DATE}"
LOCAL_DIR = "/home/lab01/airflow/reviews_data"
BATCH_SIZE = 5  # 50개씩 처리하여 메모리 사용 최소화

# 디렉토리 생성
os.makedirs(LOCAL_DIR, exist_ok=True)

def get_product_ids():
    """MySQL에서 모든 product_id 조회"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT product_id FROM products")
    product_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return product_ids

def get_existing_review_ids():
    print("[INFO] MySQL에서 기존 리뷰 ID 조회 중...")
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT review_id FROM reviews")
    existing_review_ids = set(row[0] for row in cursor.fetchall())
    cursor.close()
    conn.close()
    print(f"[INFO] {len(existing_review_ids)}개의 기존 리뷰 조회 완료")
    return existing_review_ids

def convert_review_date(date_text):
    """후기 날짜 변환 ('N일 전' -> 'YYYY-MM-DD')"""
    today = datetime.today()

    try:
        if "오늘" in date_text:
            return today.strftime("%Y-%m-%d")
        if "어제" in date_text:
            return (today - timedelta(days=1)).strftime("%Y-%m-%d")
        if "일 전" in date_text:
            days_ago = int(date_text.replace("일 전", "").strip())
            return (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        if "." in date_text:
            return datetime.strptime(f"20{date_text}", "%Y.%m.%d").strftime("%Y-%m-%d")
    except ValueError:
        print(f"[ERROR] 날짜 변환 실패: {date_text}")
        return None  # 날짜 변환 실패 시 None 반환

    return None

# def convert_review_date(date_text):
#     """후기 날짜 변환 ('N일 전' -> 'YYYY-MM-DD')"""
#     today = datetime.today()
#     yesterday = today - timedelta(days=1)  # 어제 날짜 기준
    
#     try:
#         if "오늘" in date_text:
#             return today.strftime("%Y-%m-%d")
#         elif "어제" in date_text:
#             return yesterday.strftime("%Y-%m-%d")
#         elif "일 전" in date_text:
#             days_ago = int(date_text.replace("일 전", "").strip())
#             return (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
#         elif "." in date_text:
#             parsed_date = datetime.strptime(f"20{date_text}", "%Y.%m.%d")
#             return parsed_date.strftime("%Y-%m-%d")
#     except Exception as e:
#         print(f"[ERROR] 날짜 변환 실패: {date_text}, 오류: {e}")
#         return date_text
#     return date_text

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import traceback

def crawling_today_reviews(product_id):
    """어제 날짜 기준으로 최신순 정렬 후 리뷰 크롤링"""
    print(f"[INFO] {product_id} 리뷰 크롤링 시작 (어제 날짜 기준)")
    
    # yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # stop_date = yesterday
    today = datetime.now().strftime("%Y-%m-%d")
    stop_date = today

    # WebDriver 설정
    try:
        print("[INFO] WebDriver 설정 시작")
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

        service = Service("/usr/bin/chromedriver")  # chromedriver 절대 경로 지정
        driver = webdriver.Chrome(service=service, options=chrome_options)
        url_review = f"https://www.musinsa.com/review/goods/{product_id}"
        driver.get(url_review)
        print("[INFO] WebDriver 초기화 성공")
        sys.stdout.flush()

    except Exception as e:
        print(f"[ERROR] WebDriver 초기화 실패: {e}")
        traceback.print_exc()
        return []


    reviews = []
    seen_review_ids = set()

    try:
        # 최신순 정렬
        time.sleep(1)
        try:
            sort_button_xpaths = [
                '//*[@id="commonLayoutContents"]/section/div/div[4]/div[5]/div[2]/div/div/button',
                '//*[@id="commonLayoutContents"]/section/div/div[3]/div[5]/div[2]/div/div/button',
                '//*[@id="commonLayoutContents"]/section/div/div[2]/div[5]/div[2]/div/div/button',
                '//*[@id="commonLayoutContents"]/section/div/div[5]/div[5]/div[2]/div/div/button',
                '//*[@id="commonLayoutContents"]/section/div/div[1]/div[5]/div[2]/div/div/button'
            ]
            sort_button = None
            for xpath in sort_button_xpaths:
                try:
                    sort_button = driver.find_element(By.XPATH, xpath)
                    if sort_button:
                        sort_button.click()
                        time.sleep(0.7)
                        break
                except:
                    continue
            if not sort_button:
                print("리뷰가 0개인 것으로 추정, 만약 0개가 아니라면 알려주세요!")
                return []

            lowrating_sort_xpaths = [
                '//*[@id="commonLayoutContents"]/section/div/div[4]/div[5]/div[2]/div/div[2]/div[2]',
                '//*[@id="commonLayoutContents"]/section/div/div[3]/div[5]/div[2]/div/div[2]/div[2]',
                '//*[@id="commonLayoutContents"]/section/div/div[2]/div[5]/div[2]/div/div[2]/div[2]',
                '//*[@id="commonLayoutContents"]/section/div/div[1]/div[5]/div[2]/div/div[2]/div[2]',
                '//*[@id="commonLayoutContents"]/section/div/div[5]/div[5]/div[2]/div/div[2]/div[2]'

            ]
            lowrating_sort_option = None
            for xpath in lowrating_sort_xpaths:
                try:
                    lowrating_sort_option = driver.find_element(By.XPATH, xpath)
                    if lowrating_sort_option:
                        lowrating_sort_option.click()
                        time.sleep(0.7)
                        break
                except:
                    continue
            if not lowrating_sort_option:
                print("최신순 옵션을 찾을 수 없음")
                return []
            print("최신순 정렬 완료!")
        except Exception as e:
            print(f"최신순 정렬 실패: {e}")
            return []



        # ✅ 크롤링 시작
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            review_blocks = driver.find_elements(By.XPATH, '//*[@id="commonLayoutContents"]/section/div/div/div[8]/div/div[1]/div')

            for block in review_blocks:
                try:
                    ##-----------------------------------------------##
                    # 리뷰 ID 크롤링
                    try:
                        review_id = block.get_attribute("data-content-id")
                        if not review_id:
                            review_id = block.get_attribute("outerHTML").split('data-content-id="')[1].split('"')[0]
                    except:
                        review_id = "리뷰 ID 없음"

                    # 중복 체크 (이미 저장된 리뷰 ID면 스킵)
                    if review_id in seen_review_ids:
                        continue
                    seen_review_ids.add(review_id)

                    ##-----------------------------------------------##
                    # 리뷰 날짜 크롤링 & 중단 조건 체크
                    try:
                        raw_date = block.find_element(By.XPATH, './/div/div[1]/a/div[2]/div[1]/span[2]').text.strip()
                        review_date = convert_review_date(raw_date)
                    except:
                        review_date = "날짜 없음"

                    # 한 줄에서 리뷰 개수 & 최근 날짜만 갱신 (출력 개선)
                    print(f"\r현재까지 추출한 리뷰 수: {len(reviews)} | 현재 리뷰 날짜: {review_date}", end="", flush=True)

                    if review_date != "날짜 없음" and review_date < today:
                        print(f"\n{review_date} (기준 {today}보다 이전) → 크롤링 종료")
                        driver.quit()
                        return reviews

                    ##-----------------------------------------------##
                    # 평점 크롤링
                    rating = "0"
                    rating_xpaths = ['.//div/div[1]/a/div[2]/div[2]/span', './/div[1]/a/div[2]/div[2]/span']
                    for rating_xpath in rating_xpaths:
                        try:
                            rating = block.find_element(By.XPATH, rating_xpath).text
                            break
                        except:
                            continue

                    ##-----------------------------------------------##
                    # 후기 내용 크롤링
                    review_text = "내용 없음"
                    review_xpaths = ['.//div/div[3]/div[3]/div/div/div[2]/span',
                                     './/div/div[3]/div[3]/div/div/div/span',
                                     './/div/div[3]/div[2]/div/div/div[2]/span',
                                     './/div/div[3]/div[2]/div/div/div/span']
                    for review_xpath in review_xpaths:
                        try:
                            review_text = block.find_element(By.XPATH, review_xpath).text
                            break
                        except:
                            continue

                    ##-----------------------------------------------##
                    # 후기 사이즈(옵션) 크롤링, 조건식으로 후기 사이즈 조건만 불러오도록 수정
                    review_size = "사이즈 정보 없음"
                    try:
                        # 우선순위 별로 가능한한 XPath를 리스트로 저장
                        xpath_list = ['.//div/div[2]/div/span','.//div/div[2]/div/span[2]']


                        for xpath in xpath_list:
                            size_elements = block.find_elements(By.XPATH, xpath)  # XPath 리스트 순회
                            for size_element in size_elements:
                                extracted_size = size_element.text.strip()

                                # "~~ 구매" 형식인지 체크하여 저구매로 끝나야만 저장
                                if extracted_size.endswith(" 구매"):
                                    review_size = extracted_size
                                    break  # 첫 번째 유효한 사이즈 값 저장 후 종료

                            if review_size != "사이즈 정보 없음" : break  # 유효한 사이즈 값을 찾았으면 XPath 탐색 종료
                    except Exception : pass  # 사이즈 정보가 없으면 기본값 유지
                    ##-----------------------------------------------##
                    # 데이터 저장
                    reviews.append([review_id, product_id, rating, review_text, review_date, review_size])

                except Exception as e:
                    print(f"리뷰 블록 크롤링 실패: {e}")
                    continue

            # 스크롤 다운 & 새로운 높이 비교
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            new_height = driver.execute_script("return document.body.scrollHeight")

            # 지정된 날짜보다 이전 리뷰가 나오면 스크롤 중단
            if review_date < stop_date:
                print(f"\n{review_date} → 중단 조건 충족 (스크롤 멈춤)")
                break

            if new_height == last_height:break
            last_height = new_height

    except Exception as e:
        print(f"크롤링 실패: {e}")
        reviews = []

    finally:driver.quit()

    return reviews



import subprocess
import traceback

def check_hdfs_connection():
    """HDFS 연결 상태 확인"""
    print("[INFO] HDFS 연결 상태 확인 중...")
    try:
        hdfs_status = subprocess.run("hdfs dfsadmin -report", shell=True, capture_output=True, text=True)
        print(f"[DEBUG] HDFS 상태 보고서:\n{hdfs_status.stdout}\n{hdfs_status.stderr}")
    except Exception as e:
        print(f"[ERROR] HDFS 상태 확인 실패: {e}")

import os
import subprocess
import pandas as pd

def save_to_hdfs(reviews):
    """HDFS에 기존 데이터 유지하면서 새로운 데이터 추가 저장"""
    file_name = f"reviews_{CURRENT_DATE}.parquet"
    local_path = os.path.join(LOCAL_DIR, file_name)
    hdfs_path = f"{HDFS_REVIEWS_DIR}/{file_name}"

    if not reviews:
        print("[INFO] 크롤링된 리뷰가 없어 빈 Parquet 파일을 생성합니다.")
        df_reviews = pd.DataFrame(columns=["review_id", "product_id", "rating", "review_text", "review_date", "review_size"])
    else:
        df_reviews = pd.DataFrame(reviews, columns=["review_id", "product_id", "rating", "review_text", "review_date", "review_size"])

    print(f"[INFO] HDFS 저장 시작 (누적 저장 모드): {hdfs_path}")

    # 기존 HDFS 파일 확인
    check_file_cmd = f"hdfs dfs -test -e {hdfs_path}"
    file_exists = subprocess.run(check_file_cmd, shell=True, capture_output=True, text=True).returncode == 0

    if file_exists:
        print(f"[INFO] 기존 HDFS 파일 존재: {hdfs_path}, 데이터 병합 진행 중...")
        temp_local_path = os.path.join(LOCAL_DIR, f"temp_{file_name}")

        # 기존 파일 다운로드
        get_cmd = f"hdfs dfs -get {hdfs_path} {temp_local_path}"
        get_result = subprocess.run(get_cmd, shell=True, capture_output=True, text=True)
        print(f"[DEBUG] HDFS get 실행 결과: {get_result.stdout}\n{get_result.stderr}")

        if os.path.exists(temp_local_path):
            print(f"[INFO] 기존 HDFS 파일 {temp_local_path} 병합 진행")
            df_existing = pd.read_parquet(temp_local_path)
            df_reviews = pd.concat([df_existing, df_reviews], ignore_index=True)

    # 중복 제거 후 Parquet 파일 저장
    df_reviews = df_reviews.drop_duplicates(subset=["review_id"], keep="last")

    df_reviews.to_parquet(local_path, engine="pyarrow", index=False)
    print(f"[INFO] 로컬 Parquet 저장 완료: {local_path}")

    # HDFS 디렉토리 생성 (없다면)
    subprocess.run(f"hdfs dfs -mkdir -p {HDFS_REVIEWS_DIR}", shell=True)
    subprocess.run(f"hdfs dfs -chmod -R 775 {HDFS_REVIEWS_DIR}", shell=True)

    # 기존 파일 삭제 후 업로드
    subprocess.run(f"hdfs dfs -rm -f {hdfs_path}", shell=True)
    put_cmd = f"hdfs dfs -put {local_path} {hdfs_path}"
    put_result = subprocess.run(put_cmd, shell=True, capture_output=True, text=True)
    print(f"[DEBUG] HDFS 업로드 결과: {put_result.stdout}\n{put_result.stderr}")

    # 업로드 후 HDFS 파일 목록 및 크기 확인
    list_cmd = f"hdfs dfs -ls {HDFS_REVIEWS_DIR}"
    subprocess.run(list_cmd, shell=True, capture_output=True, text=True)

    du_cmd = f"hdfs dfs -du -h {hdfs_path}"
    subprocess.run(du_cmd, shell=True, capture_output=True, text=True)

    print(f"[INFO] HDFS 저장 완료 (누적 저장 모드): {hdfs_path}")



def create_hdfs_directory():
    """HDFS 디렉토리가 없으면 생성 및 상태 확인"""
    print("[INFO] HDFS 연결 상태 확인 중...")
    hdfs_status = subprocess.run("hdfs dfsadmin -report", shell=True, capture_output=True, text=True)
    print(f"[DEBUG] HDFS 상태 보고서:\n{hdfs_status.stdout}\n{hdfs_status.stderr}")

    check_dir_cmd = f"hdfs dfs -test -d {HDFS_REVIEWS_DIR}"
    dir_exists = subprocess.run(check_dir_cmd, shell=True, capture_output=True, text=True).returncode == 0

    if not dir_exists:
        print(f"[INFO] HDFS 디렉토리 생성: {HDFS_REVIEWS_DIR}")
        subprocess.run(f"hdfs dfs -mkdir -p {HDFS_REVIEWS_DIR}", shell=True)
        subprocess.run(f"hdfs dfs -chmod -R 775 {HDFS_REVIEWS_DIR}", shell=True)
    else:
        print(f"[INFO] HDFS 디렉토리 존재: {HDFS_REVIEWS_DIR}")

def update_reviews():
    """무한 루프 실행하여 크롤링 지속"""
    while True:  # 무한 실행
        product_ids = get_product_ids()[::-1]
        existing_review_ids = get_existing_review_ids()

        # HDFS 디렉토리 생성 (처음 한 번만 실행)
        create_hdfs_directory()

        for i in range(0, len(product_ids), BATCH_SIZE):
            batch_product_ids = product_ids[i:i + BATCH_SIZE]

            all_reviews = []
            for product_id in batch_product_ids:
                reviews = crawling_today_reviews(product_id)  # ✅ 기존 crawl_reviews → crawling_today_reviews 변경

                # 기존 리뷰 필터링 (중복 방지)
                new_reviews = [review for review in reviews if review[0] not in existing_review_ids]
                existing_review_ids.update([review[0] for review in new_reviews])

                all_reviews.extend(new_reviews)

            save_to_hdfs(all_reviews)

            # 각 배치 완료 후 process_pyspark.py 실행
            spark_cmd = f"spark-submit --jars /usr/local/lib/mysql-connector-j-8.0.33.jar /home/lab01/airflow/crawling_process/process_pyspark.py"
            try:
                spark_result = subprocess.run(spark_cmd, shell=True, capture_output=True, text=True)
                print(f"[INFO] Spark 실행 결과:\n{spark_result.stdout}\n{spark_result.stderr}")
            except Exception as e:
                print(f"[ERROR] Spark 실행 중 오류 발생: {e}\n{traceback.format_exc()}")

            print(f"[INFO] {len(all_reviews)}개의 리뷰를 HDFS에 저장하고 MySQL 적재 완료.")

            gc.collect()

        print("[INFO] 모든 상품의 리뷰 크롤링 완료. 10분 후 재시작...")
        time.sleep(600)  # 10분 대기 후 다시 크롤링 시작

if __name__ == "__main__":
    update_reviews()


