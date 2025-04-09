import os
import re
import pymysql
import torch
import kss
from datetime import datetime
from transformers import BertTokenizer, BertForSequenceClassification, AutoTokenizer, AutoModelForSequenceClassification

# 모델 불러오기
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 카테고리 분류 모델
category_model_path = "/home/lab01/airflow/model/best_model.pth"
sentiment_model_path = "/home/lab01/airflow/model/best_model_with_val.pth"

try:
    # 카테고리 분류 모델 (`klue/bert-base`)
    print("[INFO] 카테고리 분류 모델 로드 중...")
    category_tokenizer = BertTokenizer.from_pretrained("klue/bert-base")
    category_model = BertForSequenceClassification.from_pretrained("klue/bert-base", num_labels=6)

    # 모델 저장 방식 확인 후 로드
    category_model_data = torch.load(category_model_path, map_location=device)
    if isinstance(category_model_data, dict):
        category_model.load_state_dict(category_model_data)
    else:
        category_model = category_model_data  # 전체 모델 저장된 경우

    category_model.to(device)
    category_model.eval()
    print("[INFO] 카테고리 분류 모델 로드 완료")

    # 감성 분석 모델 (`monologg/kobert`)
    print("[INFO] 감성 분석 모델 로드 중...")
    sentiment_tokenizer = AutoTokenizer.from_pretrained("monologg/kobert", trust_remote_code=True)
    sentiment_model_data = torch.load(sentiment_model_path, map_location=device)
    
    # 모델 저장 방식 확인 후 로드
    if isinstance(sentiment_model_data, dict):
        sentiment_model = AutoModelForSequenceClassification.from_pretrained(
            "monologg/kobert",
            num_labels=3,
            trust_remote_code=True  # 신뢰 코드 실행 옵션 추가
        )
        sentiment_model.load_state_dict(sentiment_model_data, strict=False)  # 🔹 strict=False 옵션 추가
    else:
        sentiment_model = sentiment_model_data  # 전체 모델 저장된 경우
    
    sentiment_model.to(device)
    sentiment_model.eval()
    print("[INFO] 감성 분석 모델 로드 완료")

except Exception as e:
    print(f"[ERROR] 모델 로드 실패: {e}")
    exit(1)

# 카테고리 목록
CATEGORIES = ["가성비", "내구성 및 품질", "디자인", "배송 및 포장 및 응대", "사이즈", "착용감"]

# 불용어 리스트
stopwords = set([
    "아", "휴", "아이구", "아이쿠", "아이고", "어", "나", "우리", "저희", "따라", "의해", "을", "를", "에", "의", "가", "으로", "로", "에게",
    "뿐이다", "의거하여", "근거하여", "입각하여", "기준으로", "예하면", "예를 들면", "예를 들자면", "저", "소인", "소생", "저희", "지말고",
    "하지마", "하지마라", "다른", "물론", "또한", "그리고", "비길수 없다", "해서는 안된다", "뿐만 아니라", "만이 아니다", "만은 아니다", "막론하고",
    "관계없이", "그치지 않다", "그러나", "그런데", "하지만", "든간에", "논하지 않다", "따지지 않다", "설사", "비록", "더라도", "아니면", "만 못하다",
    "하는 편이 낫다", "불문하고", "향하여", "향해서", "향하다", "쪽으로", "틈타", "이용하여", "타다", "오르다", "제외하고", "이 외에", "이 밖에",
    "하여야", "비로소", "한다면 몰라도", "외에도", "이곳", "여기", "부터", "기점으로", "따라서", "할 생각이다", "하려고하다", "이리하여", "그리하여",
    "그렇게 함으로써", "하지만", "일때", "할때", "앞에서", "중에서", "보는데서", "으로써", "로써", "까지", "해야한다", "일것이다", "반드시",
    "할줄알다", "할수있다", "할수있어", "임에 틀림없다", "한다면", "등", "등등", "제", "겨우", "단지", "다만", "할뿐", "딩동", "댕그", "대해서",
    "대하여", "대하면", "훨씬", "얼마나", "얼마만큼", "얼마큼", "남짓", "여", "얼마간", "약간", "다소", "좀", "조금", "다수", "몇", "얼마",
    "지만", "하물며", "또한", "그러나", "그렇지만", "하지만", "이외에도", "대해 말하자면", "뿐이다", "다음에", "반대로", "반대로 말하자면",
    "이와 반대로", "바꾸어서 말하면", "바꾸어서 한다면", "만약", "그렇지않으면", "까악", "툭", "딱", "삐걱거리다", "보드득", "비걱거리다", "꽈당",
    "응당", "해야한다", "에 가서", "각", "각각", "여러분", "각종", "각자", "제각기", "하도록하다", "와", "과", "그러므로", "그래서", "고로",
    "한 까닭에", "하기 때문에", "거니와", "이지만", "대하여", "관하여", "관한", "과연", "실로", "아니나다를가", "생각한대로", "진짜로", "한적이있다",
    "하곤하였다", "하", "하하", "허허", "아하", "거바", "와", "오", "왜", "어째서", "무엇때문에", "어찌", "하겠는가", "무슨", "어디", "어느곳",
    "더군다나", "하물며", "더욱이는", "어느때", "언제", "야", "이봐", "어이", "여보시오", "흐흐", "흥", "휴", "헉헉", "헐떡헐떡", "영차", "여차",
    "어기여차", "끙끙", "아야", "앗", "아야", "콸콸", "졸졸", "좍좍", "뚝뚝", "주룩주룩", "솨", "우르르", "그래도", "또", "그리고", "바꾸어말하면",
    "바꾸어말하자면", "혹은", "혹시", "답다", "및", "그에 따르는", "때가 되어", "즉", "지든지", "설령", "가령", "하더라도", "할지라도", "일지라도",
    "지든지", "몇", "거의", "하마터면", "인젠", "이젠", "된바에야", "된이상", "만큼", "어찌됏든", "그위에", "게다가", "점에서 보아", "비추어 보아",
    "고려하면", "하게될것이다", "일것이다", "비교적", "좀", "보다더", "비하면", "시키다", "하게하다", "할만하다", "의해서", "연이서", "이어서",
    "잇따라", "뒤따라", "뒤이어", "결국", "의지하여", "기대여", "통하여", "자마자", "더욱더", "불구하고", "얼마든지", "마음대로", "주저하지 않고",
    "곧", "즉시", "바로", "당장", "하자마자", "밖에 안된다", "하면된다", "그래", "그렇지", "요컨대", "다시 말하자면", "바꿔 말하면", "즉", "구체적으로", 
    '너무', '더보기', '히히', '크크', '케케', '그냥', '잉', '앙', '엉', '옹', '하', '야', '이야'
])

# MySQL 연결 정보
DB_CONFIG = {
    "host": "15.152.242.221",
    "user": "admin",
    "password": "Admin@1234",
    "database": "musinsa_pd_rv"
}

# 기존 테이블 삭제
def reset_table():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        TODAY = datetime.today().strftime("%Y%m%d")
        TABLE_NAME = f"today_reviews_{TODAY}_analysis"

        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn.commit()
        print(f"[INFO] 기존 분석 테이블 삭제 완료: {TABLE_NAME}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] 테이블 삭제 실패: {e}")
        exit(1)

# 리뷰 데이터 가져오기
def get_new_reviews():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        TODAY = datetime.today().strftime("%Y-%m-%d")

        query = f"""
        SELECT review_id, product_id, review_text, review_date, rating
        FROM reviews WHERE review_date = '{TODAY}'
        """
        cursor.execute(query)
        reviews = cursor.fetchall()

        cursor.close()
        conn.close()
        print(f"[INFO] {len(reviews)}개의 리뷰 데이터를 가져왔습니다.")
        return reviews
    except Exception as e:
        print(f"[ERROR] MySQL 데이터 조회 실패: {e}")
        exit(1)

# 텍스트 정제 함수
def clean_text(text):
    text = str(text)

    # ㅋㅋㅋ, ㅎㅎㅎ, ㅠㅠㅠ 같은 감탄사 제거
    text = re.sub(r"ㅋ{2,}|ㅎ{2,}|ㅠ{2,}|ㅜ{2,}", "", text)
    
    # 한글 및 공백만 남기고 제거
    text = re.sub(r"[^가-힣\s]", "", text)
    
    # 불용어 제거
    words = text.split()
    words = [word for word in words if word not in stopwords]
    
    # 정제된 단어들을 다시 합침
    text = " ".join(words)
    
    # 공백 정리
    text = re.sub(r"\s+", " ", text).strip()
    
    return text

# 문장 분리
def split_sentences(review):
    return kss.split_sentences(review)

# 리뷰 카테고리 예측 (다중 라벨 분류)
def predict_category(sentence):
    inputs = category_tokenizer(sentence, padding="max_length", truncation=True, max_length=128, return_tensors="pt").to(device)

    with torch.no_grad():
        outputs = category_model(**inputs)

    probs = torch.sigmoid(outputs.logits)
    predictions = (probs > 0.5).int()

    predicted_labels = [CATEGORIES[j] for j in range(len(CATEGORIES)) if predictions[0][j] == 1]
    
    return predicted_labels if predicted_labels else ["기타"]

# 감성 분석 예측 (부정=0, 중립=1, 긍정=2)
def predict_sentiment(sentence):
    inputs = sentiment_tokenizer(sentence, padding="max_length", truncation=True, max_length=128, return_tensors="pt").to(device)

    with torch.no_grad():
        outputs = sentiment_model(**inputs)

    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    sentiment_class = torch.argmax(probs, dim=-1).item()  #0: 부정, 1: 중립, 2: 긍정
    
    # 감성 분석 결과를 텍스트로 변환할 수도 있음
    sentiment_labels = {0: "부정", 1: "중립", 2: "긍정"}
    return sentiment_class  # 또는 sentiment_labels[sentiment_class]로 반환 가능


# 분석 결과 저장
def save_results_to_mysql(results):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        TODAY = datetime.today().strftime("%Y%m%d")
        TABLE_NAME = f"today_reviews_{TODAY}_analysis"

        # ✅ `today_reviews_{TODAY}_analysis` 테이블 생성 (기존 로직 유지)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            review_id VARCHAR(255),
            product_id VARCHAR(255),
            sentence VARCHAR(255),
            category VARCHAR(255),
            sentiment INT,
            rating FLOAT,
            review_date DATE,
            PRIMARY KEY (review_id, sentence)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # ✅ `review_analysis` 테이블에도 동일한 구조로 저장
        create_table_query_review_analysis = """
        CREATE TABLE IF NOT EXISTS review_analysis (
            review_id VARCHAR(100),
            product_id VARCHAR(50),
            sentence TEXT,
            category VARCHAR(255),
            sentiment INT,
            rating FLOAT,
            review_date DATE,
            PRIMARY KEY (review_id, category)
        );
        """
        cursor.execute(create_table_query_review_analysis)
        conn.commit()

        # ✅ `today_reviews_{TODAY}_analysis` 테이블에 데이터 저장
        insert_query_today = f"""
        INSERT INTO {TABLE_NAME} (review_id, product_id, sentence, category, sentiment, rating, review_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE category = VALUES(category), sentiment = VALUES(sentiment);
        """
        cursor.executemany(insert_query_today, results)
        conn.commit()
        print(f"[INFO] {len(results)}개의 분석 결과 저장 완료 ({TABLE_NAME})")

        # ✅ `review_analysis` 테이블에도 동일한 데이터 삽입
        insert_query_review_analysis = """
        INSERT INTO review_analysis (review_id, product_id, sentence, category, sentiment, rating, review_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE category = VALUES(category), sentiment = VALUES(sentiment);
        """
        cursor.executemany(insert_query_review_analysis, results)
        conn.commit()
        print(f"[INFO] {len(results)}개의 분석 결과 저장 완료 (review_analysis)")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] MySQL 데이터 저장 실패: {e}")
        exit(1)

# 실행
if __name__ == "__main__":
    reset_table()
    new_reviews = get_new_reviews()

    if new_reviews:
        results = []
        for review_id, product_id, review_text, review_date, rating in new_reviews:
            cleaned_text = clean_text(review_text)
            for sentence in split_sentences(cleaned_text):
                categories = predict_category(sentence)
                sentiment = predict_sentiment(sentence)

                results.append((review_id, product_id, sentence[:255], ",".join(categories), sentiment, rating, review_date))

        save_results_to_mysql(results)
