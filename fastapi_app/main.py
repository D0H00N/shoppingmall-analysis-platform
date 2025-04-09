from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Any
from collections import Counter
import pymysql
from datetime import datetime, timedelta, date
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
import aioredis
import re
import aiohttp
import aiomysql
from fastapi.responses import JSONResponse  # ✅ 추가
from aiomysql.cursors import DictCursor  # ✅ 올바른 import
import calendar

app = FastAPI()

# 정적 파일 서빙 (이미지, CSS 등)
app.mount("/static", StaticFiles(directory="static"), name="static")

# 템플릿 설정 (HTML 파일 위치)
templates = Jinja2Templates(directory="templates")

# MySQL 연결 정보
DB_CONFIG = {
    "host": "15.152.242.221",
    "user": "admin",
    "password": "Admin@1234",
    "database": "musinsa_pd_rv",  # ✅ 데이터베이스 확인
    "cursorclass": pymysql.cursors.DictCursor,
}

# CORS 설정 (필수)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인 허용 (보안 설정 필요)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DateFilter(BaseModel):
    year: Optional[int] = None  # ✅ Optional[int]으로 변경
    month: Optional[int] = None
    day: Optional[int] = None


# 테이블명 자동 설정
TODAY_DATE = datetime.today().strftime("%Y%m%d")
TABLE_NAME = f"today_reviews_{TODAY_DATE}_analysis"


### 🔹 MySQL 연결 함수
def get_mysql_connection():
    try:
        return pymysql.connect(**DB_CONFIG)
    except pymysql.MySQLError as e:
        print(f"MySQL Connection Error: {e}")
        return None

# 기본 페이지 렌더링 (index.html)
@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
#--------------------------------------------------------#
# 사이즈 분석 페이지 렌더링 (size_analysis.html)
@app.get("/size_analysis.html")
async def size_analysis_page(request: Request):
    return templates.TemplateResponse("size_analysis.html", {"request": request})

# ✅ review_size 전처리 함수
def preprocess_review_size(size):
    size = size.strip()

    # 1️⃣ 괄호 안 숫자 중에서 가장 큰 값 추출 (ex: (19)Goofy Black · 225 구매 -> 225)
    bracket_match = re.findall(r'\((\d+)\)', size)
    if bracket_match:
        return max(bracket_match, key=int)  # 가장 큰 숫자를 반환 (색상 코드보다 사이즈가 더 클 가능성 높음)

    # 2️⃣ '~' 포함된 경우 → 첫 번째 숫자 추출 (ex: 230~240 -> 230)
    range_match = re.search(r'(\d+)~\d+', size)
    if range_match:
        return range_match.group(1)

    # 3️⃣ 숫자 + 소수점 있는 경우 (ex: 40.5 등)
    num_match = re.search(r'\b\d+(\.\d+)?\b', size)
    if num_match:
        return num_match.group(0)

    # 4️⃣ 알파벳 사이즈 추출 (M, L, S, XS, XL, XXL 등)
    alpha_match = re.search(r'\b(M|L|S|XS|XL|XXL)\b', size, re.IGNORECASE)
    if alpha_match:
        return alpha_match.group(0).upper()

    # 5️⃣ 추가 패턴 처리: 구매 문구 포함된 경우 숫자 추출 (ex: "DARK GREY(401) · 225 구매" → "225")
    purchase_match = re.search(r'(\d{2,3})\s*구매', size)
    if purchase_match:
        return purchase_match.group(1)
    
    # 6️⃣ 한글과 숫자가 섞여 있는 경우 → 숫자만 추출 (ex: "블랙 225" → "225")
    mixed_match = re.findall(r'\d{2,3}', size)
    if mixed_match:
        return max(mixed_match, key=int)  # 가장 큰 숫자를 반환

    # 6️⃣ 숫자도 없고 알파벳 사이즈도 없으면 "-"
    return "-"

# ✅ 1. 카테고리별 부정 비율 50% 이상 리스트
def find_most_negative_categories(results):
    category_negatives = {}

    for row in results:
        category = row.get("category")
        negative_ratio = row["negative_ratio"]

        if negative_ratio >= 50:
            if category not in category_negatives:
                category_negatives[category] = []
            category_negatives[category].append(row)

    return [item for sublist in category_negatives.values() for item in sublist]

# ✅ 2. 브랜드별 부정 비율 50% 이상 리스트
def find_most_negative_brands(results):
    brand_negatives = {}

    for row in results:
        brand = row.get("brand")
        negative_ratio = row["negative_ratio"]

        if negative_ratio >= 50:
            if brand not in brand_negatives:
                brand_negatives[brand] = []
            brand_negatives[brand].append(row)

    return [item for sublist in brand_negatives.values() for item in sublist]

# ✅ 3. 전체에서 부정 비율이 가장 높은 사이즈 리스트
def find_most_negative_sizes(results):
    if not results:
        return []

    max_negative = max(row["negative_ratio"] for row in results)

        # if max_negative >= 50:
    return [row for row in results if row["negative_ratio"] == max_negative]
        # return []

# 사이즈 분석 API
@app.get("/api/size-analysis")
async def size_analysis() -> Dict[str, Any]:  # 타입을 Dict[str, Any]로 수정
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        query = """
         SELECT 
            s.product_id, s.review_size, s.total_reviews, s.avg_rating, s.negative_ratio, 
            p.product_name, p.brand, p.category, p.gender, p.price, p.image_url,
            -- ✅ 중립(0) 또는 부정(1) 리뷰만 가져오기 (최대 5개)
            (SELECT GROUP_CONCAT(DISTINCT r.sentence ORDER BY r.review_id DESC SEPARATOR ' || ') 
            FROM review_analysis r 
            WHERE r.product_id = s.product_id 
            AND r.category LIKE '%사이즈%'  
            AND r.sentiment IN (0,1)  
            LIMIT 5) AS reviews
        FROM size_analysis_view s
        JOIN products p ON s.product_id = p.product_id
        GROUP BY s.product_id, s.review_size, s.total_reviews, s.avg_rating, s.negative_ratio, 
                p.product_name, p.brand, p.category, p.gender, p.price, p.image_url;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            raise HTTPException(status_code=404, detail="No data found in size_analysis_view")
 
         # ✅ review_size 전처리 적용
        for row in results:
            row["review_size"] = preprocess_review_size(row["review_size"])


        # 🔹 최종 결과 저장
        most_negative_categories = find_most_negative_categories(results)
        most_negative_brands = find_most_negative_brands(results)
        most_negative_sizes = find_most_negative_sizes(results)
            
        # 가장 많이 등장한 사이즈 찾기 
        def most_frequent_size(size_list):
            if not size_list:
                return "N/A"
            size_counter = Counter(row["review_size"] for row in size_list)
            return size_counter.most_common(1)[0][0]  # e.g., "260"

        # ✅ JSON 직렬화 가능하도록 수정
        response_data = {
            "size_analysis": results,
            "most_negative_categories": most_negative_categories,
            "most_negative_brands": most_negative_brands,
            "most_negative_sizes": most_negative_sizes,
            "summary_info": {
                "category_negative_count": len(most_negative_categories),
                "most_negative_count": most_frequent_size(most_negative_sizes),
                "brand_negative_count": len(most_negative_brands),
                "most_frequent_most_negative": most_frequent_size(most_negative_sizes),
            }
        }

        cursor.close()
        conn.close()

        return response_data

    except pymysql.err.ProgrammingError as e:
        raise HTTPException(status_code=500, detail=f"MySQL Programming Error: {str(e)}")

    except pymysql.err.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"MySQL Connection Error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")


# 브랜드 분석 평가 API
@app.get("/api/brand-analysis/{brand}")
async def brand_analysis(brand: str) -> Dict[str, Any]:
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        print(f"🔍 브랜드 분석 요청: {brand}")

        # ✅ 해당 브랜드의 모든 제품 ID 조회
        product_query = """
        SELECT product_id, product_name, category
        FROM products
        WHERE brand = %s
        """
        cursor.execute(product_query, (brand,))
        products = cursor.fetchall()

        if not products:
            return {"message": "❗ No products found for this brand", "topics": {}, "raw_data": []}

        product_ids = [p["product_id"] for p in products]

        # ✅ 감성 분석 데이터 조회
        query = """
        SELECT category, sentiment 
        FROM review_analysis 
        WHERE product_id IN (%s)
        """ % ','.join(['%s'] * len(product_ids))  # IN 절을 동적으로 생성
        cursor.execute(query, tuple(product_ids))
        results = cursor.fetchall()

        # print(f"🟡 감성 분석 데이터: {results}")  

        if not results:
            return {"message": "❗ No sentiment data found for this brand", "topics": {}, "raw_data": []}


        # ✅ 감성 분석 데이터 가공
        topic_scores = {
            "내구성 및 품질": [],
            "디자인": [],
            "사이즈": [],
            "가성비": [],
            "착용감": [],
            "배송 및 포장 및 응대": []
        }
        topic_counts = {key: 0 for key in topic_scores}  

        for review in results:
            category = review["category"]
            sentiment = review["sentiment"]

            if category and sentiment is not None:
                categories = category.split(",")  
                for cat in categories:
                    cat = cat.strip()
                    if cat in topic_scores:
                        topic_scores[cat].append(sentiment)
                        topic_counts[cat] += 1  

        # ✅ 토픽별 만족도 계산
        topic_aggregated = {
            topic: {
                "score": round((sum(scores) / len(scores)) * 50, 2) if scores else 0,  # 0~100 변환
                "count": topic_counts[topic]
            }
            for topic, scores in topic_scores.items()
        }


        # ✅ 브랜드 종합 점수 계산
        review_count = len(results)  # 전체 리뷰 개수
        avg_sentiment = sum(r["sentiment"] for r in results) / review_count * 50 if review_count > 0 else 0
        # avg_rating = sum(r["sentiment"] for r in results) / review_count if review_count > 0 else 0
        # past_reviews = review_count - recent_reviews  # 과거 리뷰 개수

        # ✅ 리뷰 개수 기준 적용
        if review_count < 30:
            review_weight = 0.7  # 리뷰 개수 적으면 신뢰도 낮음(감점 크게)
        elif review_count < 100:
            review_weight = 0.85
        elif review_count < 300:
            review_weight = 0.95
        else:
            review_weight = 1.0  # 리뷰 개수가 충분하면 감점 없음

        # ✅ 최근 리뷰 비율 적용
        recent_reviews = sum(1 for r in results if r["sentiment"] >= 0.7)  # 최근 긍정 리뷰 개수
        recent_ratio = recent_reviews / max(review_count, 1)  

        if recent_ratio < 0.3:
            recent_weight = 0.85  # 긍정 리뷰 비율이 낮으면 감점
        elif recent_ratio < 0.5:
            recent_weight = 0.95
        else:
            recent_weight = 1.0  # 최근 긍정 리뷰 비율이 높으면 감점 없음

        # ✅ 특정 토픽 점수 적용
        min_topic_score = min(topic["score"] for topic in topic_aggregated.values() if topic["count"] > 0)

        if min_topic_score < 60:
            topic_weight = 0.8
        elif min_topic_score < 75:
            topic_weight = 0.9
        else:
            topic_weight = 1.0  # 특정 토픽의 점수가 높으면 감점 없음

        # # ✅ 평점 적용
        # if avg_rating and avg_rating <= 4.7:overall_score *= 0.97
        # elif avg_rating and avg_rating <= 4.6:overall_score *= 0.95

        # ✅ 최대 100점 제한
        overall_score = avg_sentiment * review_weight * recent_weight * topic_weight
        overall_score = min(100, round(overall_score, 2))
        
        def get_brand_rating(score: float) -> str:
            """브랜드 종합 평가 점수를 등급으로 변환"""
            if score < 60:
                return "미흡"
            elif score < 80:
                return "보통"
            else:
                return "좋음"

        # ✅ 부정(1) 개수 조회
        negative_query = """
        SELECT category, COUNT(*) as count
        FROM review_analysis
        WHERE product_id IN (%s) AND sentiment = 1
        GROUP BY category
        """ % ','.join(['%s'] * len(product_ids))

        cursor.execute(negative_query, tuple(product_ids))
        negative_results = cursor.fetchall()

        # ✅ 중립(0) 개수 조회
        neutral_query = """
        SELECT category, COUNT(*) as count
        FROM review_analysis
        WHERE product_id IN (%s) AND sentiment = 0
        GROUP BY category
        """ % ','.join(['%s'] * len(product_ids))

        cursor.execute(neutral_query, tuple(product_ids))
        neutral_results = cursor.fetchall()

        # ✅ 긍정(2) 개수 조회
        positive_query = """
        SELECT category, COUNT(*) as count
        FROM review_analysis
        WHERE product_id IN (%s) AND sentiment = 2
        GROUP BY category
        """ % ','.join(['%s'] * len(product_ids))

        cursor.execute(positive_query, tuple(product_ids))
        positive_results = cursor.fetchall()

        # ✅ 감성 분석 데이터 가공
        topic_sentiment = {
            "내구성 및 품질": {"positive": 0, "neutral": 0, "negative": 0, "count": 0},
            "디자인": {"positive": 0, "neutral": 0, "negative": 0, "count": 0},
            "사이즈": {"positive": 0, "neutral": 0, "negative": 0, "count": 0},
            "가성비": {"positive": 0, "neutral": 0, "negative": 0, "count": 0},
            "착용감": {"positive": 0, "neutral": 0, "negative": 0, "count": 0},
            "배송 및 포장 및 응대": {"positive": 0, "neutral": 0, "negative": 0, "count": 0}
        }

        # ✅ 부정(1) 데이터 반영
        for review in negative_results:
            category = review["category"]
            if category in topic_sentiment:
                topic_sentiment[category]["negative"] = review["count"]
                topic_sentiment[category]["count"] += review["count"]

        # ✅ 중립(0) 데이터 반영
        for review in neutral_results:
            category = review["category"]
            if category in topic_sentiment:
                topic_sentiment[category]["neutral"] = review["count"]
                topic_sentiment[category]["count"] += review["count"]

        # ✅ 긍정(2) 데이터 반영
        for review in positive_results:
            category = review["category"]
            if category in topic_sentiment:
                topic_sentiment[category]["positive"] = review["count"]
                topic_sentiment[category]["count"] += review["count"]


        # ✅ 브랜드별 리뷰 개수 (연도별 집계)
        brand_query = """
        SELECT YEAR(r.review_date) AS review_year, 
               COUNT(*) AS total_reviews
        FROM reviews r
        JOIN products p ON r.product_id = p.product_id
        WHERE p.brand = %s
        GROUP BY review_year
        ORDER BY review_year
        """

        cursor.execute(brand_query, (brand,))
        sales_data = cursor.fetchall()

        if not sales_data:
            return {"message": "❗ No sales data found", "sales": []}

        # ✅ 데이터 변환: { "2019": 150, "2020": 200, "2021": 300 }
        sales_dict = {str(row["review_year"]): row["total_reviews"] for row in sales_data} if sales_data else {}
        # ✅ 점수 저장: brand_scores 테이블에 저장

        return {
            "message": "✅ Success",
            "overall_score": overall_score,
            "rating": get_brand_rating(overall_score),  # 등급 
            "sales_data": sales_dict,
            "brand": brand,
            "topics": topic_aggregated,
            "raw_data": results,
            "topic_sentiment": topic_sentiment
        }

    except pymysql.MySQLError as e:
        print(f"🔥 MySQL Error: {str(e)}")
        return {"message": "🔥 MySQL Error", "topics": {}, "raw_data": []}

    except Exception as e:
        print(f"🔥 Unexpected Error: {str(e)}")
        return {"message": "🔥 Unexpected Error", "topics": {}, "raw_data": []}


# 브랜드 검색 API (사용자 입력한 문자열 포함하는 브랜드 목록 검색)
@app.get("/api/search-brands/")
async def search_brands(query: str):
    conn = get_mysql_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    search_query = """
    SELECT DISTINCT brand 
    FROM products 
    WHERE brand LIKE %s 
    LIMIT 10
    """
    cursor.execute(search_query, (f"%{query}%",))
    brands = cursor.fetchall()

    return {"brands": [b["brand"] for b in brands]}


#--------------------------------------------------------#

#--------------------------------------------------------#
# 강민성 - 강약점 작업


# 상품명 검색 추천 API (예상 검색어 제안)
@app.get("/api/suggest-products/{query}", response_model=List[Dict[str, Any]])
async def suggest_products(query: str):
    """
    사용자가 입력한 query(제품명 일부)를 기반으로 products 테이블에서 유사한 상품명을 검색하여 추천 목록을 반환.
    """
    # MySQL 연결
    connection = get_mysql_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        with connection.cursor() as cursor:
            # LIKE를 사용하여 유사한 상품명 검색 (대소문자 구분 없이)
            sql = """
                SELECT product_id, product_name 
                FROM products 
                WHERE product_name LIKE %s 
                LIMIT 5
            """
            # %query%로 부분 일치 검색
            search_query = f"%{query}%"
            cursor.execute(sql, (search_query,))
            results = cursor.fetchall()

            if not results:
                return []  # 검색 결과가 없으면 빈 리스트 반환

            # 결과를 JSON 형태로 반환 (product_id와 product_name만 포함)
            suggestions = [
                {
                    "product_id": row["product_id"],
                    "product_name": row["product_name"]
                }
                for row in results
            ]
            return suggestions

    except pymysql.MySQLError as e:
        print(f"MySQL Error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching product suggestions")

    finally:
        connection.close()

# 상품명으로 product_id 조회 API
@app.get("/api/get-product-id/{product_name}", response_model=Dict[str, Any])
async def get_product_id(product_name: str):
    """
    상품명을 입력받아 해당 상품의 product_id를 반환.
    """
    # MySQL 연결
    connection = get_mysql_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        with connection.cursor() as cursor:
            # 상품명으로 product_id 조회 (정확한 일치)
            sql = """
                SELECT product_id, product_name 
                FROM products 
                WHERE product_name = %s 
                LIMIT 1
            """
            cursor.execute(sql, (product_name,))
            result = cursor.fetchone()

            if not result:
                raise HTTPException(status_code=404, detail="Product not found")

            # 결과를 JSON 형태로 반환
            return {
                "product_id": result["product_id"],
                "product_name": result["product_name"]
            }

    except pymysql.MySQLError as e:
        print(f"MySQL Error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching product ID")

    finally:
        connection.close()


# id로 브랜드 점수 불러오는 코드 추가
@app.get("/api/brand-analysis-kang/{product_id}")
async def brand_analysis_kang(product_id: int) -> Dict[str, Any]:
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        print(f"🔍 브랜드 분석 요청 (상품 ID 기준): {product_id}")

        # ✅ 1. 상품 ID로 브랜드 조회
        brand_query = """
        SELECT brand FROM products WHERE product_id = %s
        """
        cursor.execute(brand_query, (product_id,))
        brand_result = cursor.fetchone()

        if not brand_result or not brand_result["brand"]:
            return {"message": "❗ No brand found for this product", "topics": {}, "raw_data": []}

        brand = brand_result["brand"]
        print(f"✅ 조회된 브랜드: {brand}")

        # ✅ 2. 기존 brand_analysis 로직 수행
        product_query = """
        SELECT product_id, product_name, category
        FROM products
        WHERE brand = %s
        """
        cursor.execute(product_query, (brand,))
        products = cursor.fetchall()

        if not products:
            return {"message": "❗ No products found for this brand", "topics": {}, "raw_data": []}

        product_ids = [p["product_id"] for p in products]

        # ✅ 감성 분석 데이터 조회
        query = """
        SELECT category, sentiment 
        FROM review_analysis 
        WHERE product_id IN (%s)
        """ % ','.join(['%s'] * len(product_ids))
        cursor.execute(query, tuple(product_ids))
        results = cursor.fetchall()

        if not results:
            return {"message": "❗ No sentiment data found for this brand", "topics": {}, "raw_data": []}

        # ✅ 기존 로직 그대로 유지 (점수 계산, 토픽 분석 등)
        topic_scores = {
            "내구성 및 품질": [],
            "디자인": [],
            "사이즈": [],
            "가성비": [],
            "착용감": [],
            "배송 및 포장 및 응대": []
        }
        topic_counts = {key: 0 for key in topic_scores}  

        for review in results:
            category = review["category"]
            sentiment = review["sentiment"]

            if category and sentiment is not None:
                categories = category.split(",")  
                for cat in categories:
                    cat = cat.strip()
                    if cat in topic_scores:
                        topic_scores[cat].append(sentiment)
                        topic_counts[cat] += 1  

        # ✅ 토픽별 만족도 계산
        topic_aggregated = {
            topic: {
                "score": round((sum(scores) / len(scores)) * 50, 2) if scores else 0,
                "count": topic_counts[topic]
            }
            for topic, scores in topic_scores.items()
        }

        # ✅ 브랜드 종합 점수 계산
        review_count = len(results)
        avg_sentiment = sum(r["sentiment"] for r in results) / review_count * 50 if review_count > 0 else 0

        if review_count < 30:
            review_weight = 0.7  
        elif review_count < 100:
            review_weight = 0.85
        elif review_count < 300:
            review_weight = 0.95
        else:
            review_weight = 1.0  

        recent_reviews = sum(1 for r in results if r["sentiment"] >= 0.7)  
        recent_ratio = recent_reviews / max(review_count, 1)  

        if recent_ratio < 0.3:
            recent_weight = 0.85  
        elif recent_ratio < 0.5:
            recent_weight = 0.95
        else:
            recent_weight = 1.0  

        min_topic_score = min(topic["score"] for topic in topic_aggregated.values() if topic["count"] > 0)

        if min_topic_score < 60:
            topic_weight = 0.8
        elif min_topic_score < 75:
            topic_weight = 0.9
        else:
            topic_weight = 1.0  

        overall_score = avg_sentiment * review_weight * recent_weight * topic_weight
        overall_score = min(100, round(overall_score, 2))
        
        def get_brand_rating(score: float) -> str:
            if score < 60:
                return "미흡"
            elif score < 80:
                return "보통"
            else:
                return "좋음"
        # ✅ brand_scores에 저장
        try:
            save_conn = get_mysql_connection()
            save_cursor = save_conn.cursor()
            save_cursor.execute("""
                REPLACE INTO brand_scores (brand, overall_score, rating, review_count)
                VALUES (%s, %s, %s, %s)
            """, (
                brand,
                overall_score,
                get_brand_rating(overall_score),
                review_count
            ))
            save_conn.commit()
            save_cursor.close()
            save_conn.close()
        except Exception as e:
            print(f"❌ 브랜드 점수 저장 실패: {brand}, 오류: {e}")
        return {
            "message": "✅ Success",
            "overall_score": overall_score,
            "rating": get_brand_rating(overall_score),  
            "brand": brand,
            "topics": topic_aggregated,
            "raw_data": results
        }

    except pymysql.MySQLError as e:
        print(f"🔥 MySQL Error: {str(e)}")
        return {"message": "🔥 MySQL Error", "topics": {}, "raw_data": []}

    except Exception as e:
        print(f"🔥 Unexpected Error: {str(e)}")
        return {"message": "🔥 Unexpected Error", "topics": {}, "raw_data": []}




# 강약점 분석 페이지 렌더링
@app.get("/strength-weakness.html")
async def strength_weakness_page(request: Request):
    return templates.TemplateResponse("strength-weakness.html", {"request": request})

@app.get("/api/strength-weakness/{product_id}") 
async def strength_weakness(product_id: str) -> Dict[str, Any]:
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)  

        # print(f"🟢 검색 요청: {product_id}")  # ✅ 요청된 상품 ID 확인

        # ✅ 제품 정보 조회
        product_query = """
        SELECT p.product_name, p.price, p.gender, p.category, p.brand, p.image_url,
               COUNT(r.review_id) as review_count, 
               AVG(r.rating) as avg_rating,
               SUM(CASE WHEN r.review_date >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH) THEN 1 ELSE 0 END) AS recent_reviews,
               SUM(CASE WHEN r.review_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 MONTH) AND DATE_SUB(CURDATE(), INTERVAL 3 MONTH) THEN 1 ELSE 0 END) AS past_reviews
        FROM products p
        LEFT JOIN review_analysis r ON p.product_id = r.product_id
        WHERE p.product_id = %s
        GROUP BY p.product_id
        """
        cursor.execute(product_query, (product_id,))
        product_info = cursor.fetchone()

        # print(f"🔵 제품 정보: {product_info}")  # ✅ 제품 정보 확인

        if not product_info:
            return {"message": "❗ No product data found", "topics": {}, "product": {}, "raw_data": []}

        # ✅ 감성 분석 데이터 조회
        query = """
        SELECT category, sentiment 
        FROM review_analysis 
        WHERE product_id = %s
        """
        cursor.execute(query, (product_id,))
        results = cursor.fetchall()

        # print(f"🟡 감성 분석 데이터: {results}")  # ✅ 감성 분석 데이터 확인

        if not results:
            return {"message": "❗ No sentiment data found", "topics": {}, "product": product_info, "raw_data": []}

        # ✅ 감성 분석 데이터 가공 (기존 기능 유지)
        topic_scores = {
            "내구성 및 품질": [],
            "디자인": [],
            "사이즈": [],
            "가성비": [],
            "착용감": [],
            "배송 및 포장 및 응대": []
        }
        topic_counts = {key: 0 for key in topic_scores}  

        for review in results:
            category = review["category"]
            sentiment = review["sentiment"]

            if category and sentiment is not None:
                categories = category.split(",")  
                for cat in categories:
                    cat = cat.strip()
                    if cat in topic_scores:
                        topic_scores[cat].append(sentiment)
                        topic_counts[cat] += 1  

        topic_aggregated = {
            topic: {
                "score": round((sum(scores) / len(scores)) * 50, 2) if scores else 0,
                "count": topic_counts[topic]
            }
            for topic, scores in topic_scores.items()
        }

        # print(f"🟠 분석 결과: {topic_aggregated}")  # ✅ 감성 분석 점수 확인

        # ✅ 추가된 종합 점수 계산 (새로운 기준 반영)
        avg_sentiment = sum(r["sentiment"] for r in results) / len(results) * 50  # 감성 점수 (0~100 변환)
        review_count = product_info["review_count"]
        avg_rating = product_info["avg_rating"]
        recent_reviews = product_info["recent_reviews"]
        past_reviews = product_info["past_reviews"]

        overall_score = avg_sentiment  # 기본 점수 (감성 점수)

        # ✅ 리뷰 개수 기준 적용
        if review_count < 50:
            overall_score *= 0.8
        elif review_count < 150:
            overall_score *= 0.9

        # ✅ 최근 리뷰 비율 적용
        if past_reviews > 0 and recent_reviews / past_reviews >= 1:
            overall_score *= 0.9

        # ✅ 특정 토픽 점수 적용
        min_topic_score = min(topic["score"] for topic in topic_aggregated.values() if topic["count"] > 0)
        if min_topic_score <= 80:
            overall_score *= 0.9

        # ✅ 평점 적용
        if avg_rating and avg_rating < 4.7:
            overall_score *= 0.95

        # ✅ 최대 100점 제한
        overall_score = min(100, round(overall_score, 2))

        # print(f"🟢 최종 종합 점수: {overall_score}")  # ✅ 확인용 로그

        # ✅ 점수 저장: product_scores 테이블에 저장
        try:
            save_conn = get_mysql_connection()
            save_cursor = save_conn.cursor()
            save_cursor.execute("""
                REPLACE INTO product_scores (product_id, product_name, brand, overall_score)
                VALUES (%s, %s, %s, %s)
            """, (
                product_id,
                product_info["product_name"],
                product_info["brand"],
                overall_score
            ))
            save_conn.commit()
            save_cursor.close()
            save_conn.close()
        except Exception as e:
            print(f"❌ 제품 점수 저장 실패: {product_id}, 오류: {e}")

        return {
            "message": "✅ Success",
            "overall_score": overall_score,  # ✅ 추가된 부분
            "topics": topic_aggregated,
            "product": {
                "product_name": product_info["product_name"],
                "price": product_info["price"],
                "gender": product_info["gender"],
                "category": product_info["category"],
                "brand": product_info["brand"],
                "image_url": product_info["image_url"],
                "review_count": product_info["review_count"],
                "avg_rating": round(product_info["avg_rating"], 2) if product_info["avg_rating"] else 0
            },
            "raw_data": results
        }

    except pymysql.MySQLError as e:
        print(f"🔥 MySQL Error: {str(e)}")
        return {"message": "🔥 MySQL Error", "topics": {}, "product": {}, "raw_data": []}

    except Exception as e:
        print(f"🔥 Unexpected Error: {str(e)}")
        return {"message": "🔥 Unexpected Error", "topics": {}, "product": {}, "raw_data": []}

MIN_REVIEW_COUNT = 30  # ✅ 최소 리뷰 개수 제한

# ✅ MySQL 연결 풀링 (초기화)
async def create_mysql_pool():
    return await aiomysql.create_pool(
        host="15.152.242.221",
        port=3306,
        user="admin",
        password="Admin@1234",
        db="musinsa_pd_rv",
        cursorclass=aiomysql.DictCursor,
        minsize=1,
        maxsize=10
    )

@app.post("/api/update-all-brand-scores")
async def update_all_brand_scores():
    if not mysql_pool:
        return {"message": "❗ DB 연결 실패"}

    async with mysql_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT DISTINCT brand FROM products WHERE brand IS NOT NULL")
            brands = await cursor.fetchall()

    if not brands:
        return {"message": "❗ 브랜드 없음"}

    for b in brands:
        brand = b["brand"]

        async with mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("SELECT product_id FROM products WHERE brand = %s", (brand,))
                product_rows = await cursor.fetchall()
                if not product_rows:
                    continue

                product_ids = [p["product_id"] for p in product_rows]
                if not product_ids:
                    continue

                format_strings = ','.join(['%s'] * len(product_ids))
                await cursor.execute(f"""
                    SELECT category, sentiment
                    FROM review_analysis
                    WHERE product_id IN ({format_strings})
                """, tuple(product_ids))
                reviews = await cursor.fetchall()

        if not reviews:
            overall_score = 0.0
            rating = "미흡"
            review_count = 0
        else:
            # 점수 계산 로직 (brand-analysis API와 동일)
            topic_scores = {
                "내구성 및 품질": [], "디자인": [], "사이즈": [],
                "가성비": [], "착용감": [], "배송 및 포장 및 응대": []
            }

            for review in reviews:
                if review["category"] and review["sentiment"] is not None:
                    for cat in review["category"].split(","):
                        cat = cat.strip()
                        if cat in topic_scores:
                            topic_scores[cat].append(review["sentiment"])

            topic_aggregated = {
                topic: {
                    "score": round((sum(scores) / len(scores)) * 50, 2) if scores else 0
                }
                for topic, scores in topic_scores.items()
            }

            review_count = len(reviews)
            avg_sentiment = sum(r["sentiment"] for r in reviews) / review_count * 50

            review_weight = 0.7 if review_count < 30 else 0.85 if review_count < 100 else 0.95 if review_count < 300 else 1.0
            recent_positive = sum(1 for r in reviews if r["sentiment"] >= 0.7)
            recent_ratio = recent_positive / review_count
            recent_weight = 0.85 if recent_ratio < 0.3 else 0.95 if recent_ratio < 0.5 else 1.0
            min_topic_score = min((t["score"] for t in topic_aggregated.values() if t["score"] > 0), default=0)
            topic_weight = 0.8 if min_topic_score < 60 else 0.9 if min_topic_score < 75 else 1.0

            overall_score = avg_sentiment * review_weight * recent_weight * topic_weight
            overall_score = min(100, round(overall_score, 2))
            rating = "미흡" if overall_score < 60 else "보통" if overall_score < 80 else "좋음"

        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    REPLACE INTO brand_scores (brand, overall_score, rating, review_count)
                    VALUES (%s, %s, %s, %s)
                """, (
                    brand,
                    overall_score,
                    rating,
                    review_count
                ))
                await conn.commit()

    return {"message": "✅ 모든 브랜드 점수 계산 및 저장 완료"}

mysql_pool = None

@app.on_event("startup")
async def startup_event():
    global mysql_pool
    mysql_pool = await create_mysql_pool()

    # 서버 시작될 때 자동 실행
    await update_all_brand_scores()

@app.on_event("shutdown")
async def shutdown_event():
    global mysql_pool
    if mysql_pool:
        mysql_pool.close()
        await mysql_pool.wait_closed()

async def fetch_rankings():
    async with mysql_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT brand, overall_score, rating, review_count
                FROM brand_scores
                WHERE review_count >= %s
                ORDER BY overall_score DESC
            """, (MIN_REVIEW_COUNT,))
            return await cursor.fetchall()

async def fetch_product_rankings():
    async with mysql_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT product_id, product_name, brand, overall_score
                FROM product_scores
                WHERE overall_score > 0
                ORDER BY overall_score DESC
            """)
            return await cursor.fetchall()

from decimal import Decimal

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    if isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    return obj

@app.get("/api/rankings")
async def get_rankings():
    brands = await fetch_rankings()
    products = await fetch_product_rankings()

    result = {
        "message": "✅ Success",
        "top_brands": convert_decimal(brands[:3]),
        "bottom_brands": convert_decimal(brands[-3:]),
        "top_products": convert_decimal(products[:3]),
        "bottom_products": convert_decimal(products[-3:])
    }

    return JSONResponse(content=result, media_type="application/json")


    
# ✅ 제품 검색 API (특정 토픽이 최고점이거나 감성 점수가 93점 이상일 때만 필터링)
@app.get("/api/product-search")
async def product_search(
    category: str = "",
    gender: str = "",
    price_min: int = 0,
    price_max: int = 99999999,
    topic: str = "",
    min_reviews: int = 0
):
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # ✅ 성별 필터링 로직 수정
        gender_filter = {
            "남자": "('남자', '공용')",
            "여자": "('여자', '공용')",
            "공용": "('남자', '여자', '공용')"
        }
        gender_condition = f"p.gender IN {gender_filter.get(gender, gender_filter['공용'])}"  # 기본값: 전체 포함

        # ✅ 1차 필터링: 제품 리스트 가져오기
        query = f"""
        SELECT p.product_id, p.product_name, p.price, p.category, p.brand, p.gender, p.image_url,
               COUNT(r.review_id) as review_count,
               AVG(r.sentiment) * 50 as avg_sentiment  -- ✅ 전체 감성 점수 평균 추가
        FROM products p
        JOIN review_analysis r ON p.product_id = r.product_id
        WHERE (p.category = %s OR %s = '')  
          AND {gender_condition}  -- ✅ 성별 필터링 수정
          AND CAST(REPLACE(p.price, ',', '') AS UNSIGNED) BETWEEN %s AND %s
        GROUP BY p.product_id
        HAVING review_count >= %s
        """
        cursor.execute(query, (category, category, price_min, price_max, min_reviews))
        product_results = cursor.fetchall()

        if not product_results:
            return {"products": []}

        product_ids = [row["product_id"] for row in product_results]

        # ✅ 2차 필터링: 감성 점수 계산 (토픽별 분리 후 감성 분석)
        sentiment_query = """
        SELECT product_id, category, sentiment
        FROM review_analysis
        WHERE product_id IN (%s)
        """ % ",".join(["%s"] * len(product_ids))

        cursor.execute(sentiment_query, product_ids)
        sentiment_results = cursor.fetchall()

        # ✅ 제품별 토픽 감성 점수 저장
        product_data = {row["product_id"]: {**row, "topics": {}} for row in product_results}

        for row in sentiment_results:
            product_id = row["product_id"]
            categories = row["category"].split(",")  # ✅ 여러 개의 토픽을 분리
            sentiment = row["sentiment"]

            for cat in categories:
                cat = cat.strip()
                if cat not in product_data[product_id]["topics"]:
                    product_data[product_id]["topics"][cat] = []
                product_data[product_id]["topics"][cat].append(sentiment)

        # ✅ 특정 토픽 필터링 적용 및 응답 데이터 구성
        filtered_products = []
        for product in product_data.values():
            topic_scores = {
                topic: round((sum(scores) / len(scores)) * 50, 2)
                for topic, scores in product["topics"].items()
            }

            product["filtered_topics"] = {}

            # ✅ 특정 토픽 필터링이 적용된 경우, 최고 점수 or 93점 이상인 경우만 포함
            if topic:
                if topic in topic_scores and (topic_scores[topic] >= 93 or topic_scores[topic] == max(topic_scores.values())):
                    product["filtered_topics"][topic] = topic_scores[topic]
                else:
                    continue  # ✅ 조건 만족하지 않으면 리스트에 추가 안함
            else:
                product["filtered_topics"] = topic_scores  # ✅ 필터링이 없을 때는 전체 포함

            product["avg_sentiment"] = round(product["avg_sentiment"], 2) if product["avg_sentiment"] else 0

            filtered_products.append(product)

        return {"products": filtered_products}

    except pymysql.MySQLError as e:
        return {"error": f"🔥 MySQL Error: {str(e)}"}
    except Exception as e:
        return {"error": f"🔥 Unexpected Error: {str(e)}"}



@app.get("/api/related-reviews/{product_id}")
async def get_related_reviews(product_id: str):
    conn = None
    cursor = None
    try:
        # ✅ MySQL 연결 확인
        conn = get_mysql_connection()
        if not conn:
            print("🔥 MySQL 연결 실패")
            return {"error": "🔥 MySQL 연결 실패"}, 500

        cursor = conn.cursor()

        print(f"🔍 리뷰 검색 요청: {product_id}")

        # ✅ 1. 상품 리뷰 개수 확인
        cursor.execute("""
            SELECT COUNT(*) AS review_count 
            FROM review_analysis 
            WHERE product_id = %s
        """, (product_id,))
        review_count = cursor.fetchone()["review_count"]

        if review_count == 0:
            print(f"🚨 해당 상품({product_id}) 리뷰 없음")
            return {"reviews": []}

        # ✅ 2. 토픽별 만족도 점수 및 빈도수 조회
        cursor.execute("""
            SELECT category, 
                   ROUND(AVG(sentiment) * 50, 2) AS score, 
                   COUNT(*) AS freq 
            FROM review_analysis
            WHERE product_id = %s
            GROUP BY category
        """, (product_id,))
        topic_data = cursor.fetchall()

        if not topic_data:
            return {"reviews": []}

        # ✅ 3. 만족도 하위 3개 + 언급량 상위 3개 토픽 포함 (모든 조건 충족)
        sorted_by_score = sorted(topic_data, key=lambda x: x["score"])[:3]
        sorted_by_freq = sorted(topic_data, key=lambda x: x["freq"], reverse=True)[:3]

        lowest_score_topics = {t["category"] for t in sorted_by_score}
        highest_freq_topics = {t["category"] for t in sorted_by_freq}

        # ✅ 4. 모든 토픽 포함 (교집합 + 개별 조건 충족한 것들)
        selected_topics = lowest_score_topics | highest_freq_topics  # 합집합

        if not selected_topics:
            print("🚨 적절한 토픽 없음")
            return {"reviews": []}

        print(f"🟢 선택된 토픽들: {selected_topics}")

        # ✅ 5. 선택된 모든 토픽을 포함하는 부정/중립 리뷰 가져오기
        topic_conditions = " OR ".join([f"ra.category LIKE '%%{topic}%%'" for topic in selected_topics])

        query = f"""
            SELECT r.review_id, r.review_text, r.rating, r.review_date, ra.sentiment
            FROM reviews r
            JOIN review_analysis ra ON r.review_id = ra.review_id
            WHERE ra.product_id = %s 
              AND ({topic_conditions})
              AND (ra.sentiment = 0 OR ra.sentiment = 1)  # 부정(0) > 중립(1) 우선
            ORDER BY ra.sentiment ASC, CHAR_LENGTH(r.review_text) DESC
            LIMIT 50
        """
        cursor.execute(query, (product_id,))
        reviews = cursor.fetchall()

        if not reviews:
            print("🚨 선택된 토픽의 부정/중립 리뷰 없음")
            return {"reviews": []}

        print(f"🟢 가져온 리뷰 개수: {len(reviews)}")

        return {"reviews": reviews}

    except pymysql.MySQLError as e:
        print(f"🔥 MySQL Error: {str(e)}")
        return {"error": "🔥 MySQL Error"}, 500

    except Exception as e:
        print(f"🔥 Unexpected Error: {str(e)}")
        return {"error": "🔥 Unexpected Error"}, 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


from fastapi import APIRouter

router = APIRouter()

from dateutil.relativedelta import relativedelta

@app.get("/api/review-trend/{product_id}")
async def get_review_trend(product_id: str):
    """상품 ID 기준으로 최근 1년 반 동안의 월별 리뷰 개수를 반환"""

    conn = None
    cursor = None

    try:
        # ✅ MySQL 연결
        conn = get_mysql_connection()
        if not conn:
            return {"error": "🔥 MySQL 연결 실패"}, 500

        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # ✅ SQL 실행 (월별 리뷰 개수 집계)
        query = """
        SELECT DATE_FORMAT(review_date, '%%Y-%%m') AS review_month, COUNT(*) AS review_count
        FROM reviews
        WHERE product_id = %s 
          AND review_date >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)
        GROUP BY review_month
        ORDER BY review_month;
        """
        cursor.execute(query, (product_id,))
        results = cursor.fetchall()

        # ✅ 결과 없으면 기본값 반환
        if not results:
            return {"product_id": product_id, "review_trend": {}}

        # ✅ 딕셔너리 변환 (빈 월 포함)
        from dateutil.relativedelta import relativedelta
        from datetime import datetime

        end_date = datetime.today().replace(day=1)  # 이번 달 1일
        start_date = end_date - relativedelta(months=18)  # 18개월 전

        trend_data = {}
        current_date = start_date

        while current_date <= end_date:
            key = current_date.strftime("%Y-%m")
            trend_data[key] = 0  # 기본값 0
            current_date += relativedelta(months=1)  # 1개월 증가

        # ✅ 결과값 반영
        for row in results:
            trend_data[row["review_month"]] = row["review_count"]

        return {"product_id": product_id, "review_trend": trend_data}

    except pymysql.MySQLError as e:
        return {"error": f"🔥 MySQL Error: {str(e)}"}, 500

    except Exception as e:
        return {"error": f"🔥 Unexpected Error: {str(e)}"}, 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()




# 추가
@app.get("/api/size-analysis-product/{product_id}")
async def get_size_analysis(product_id: str) -> Dict[str, Any]:
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        query = """
        SELECT review_size, total_reviews, positive_count, neutral_count, negative_count
        FROM size_analysis_view
        WHERE product_id = %s
        """
        cursor.execute(query, (product_id,))
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        size_data = []
        for row in results:
            total_reviews = row["total_reviews"]
            if total_reviews == 0:
                continue

            satisfaction_score = ((row["positive_count"] * 100) + (row["neutral_count"] * 50)) / total_reviews
            size_data.append({
                "review_size": row["review_size"],
                "satisfaction_score": round(satisfaction_score, 1),
                "total_reviews": total_reviews
            })

        # 상위 3개 & 하위 3개 정렬
        size_data_sorted = sorted(size_data, key=lambda x: x["satisfaction_score"], reverse=True)
        top_3 = size_data_sorted[:3]
        bottom_3 = size_data_sorted[-3:]

        return {
            "top_sizes": top_3,
            "bottom_sizes": bottom_3
        }

    except pymysql.MySQLError as e:
        return {"message": f"MySQL Error: {str(e)}"}

    except Exception as e:
        return {"message": f"Unexpected Error: {str(e)}"}

# 베이스 페이지 만들기
# 🚀 `/dashboard/`에서 base.html 불러오기
@app.get("/dashboard/")
async def dashboard_page(request: Request):
    return templates.TemplateResponse("base.html", {"request": request})

# 🚀 AJAX 요청으로 index.html 불러오기
@app.get("/dashboard/index/")
async def load_index_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# 🚀 AJAX 요청으로 strength-weakness.html 불러오기
@app.get("/dashboard/strength-weakness/")
async def load_strength_weakness_page(request: Request):
    return templates.TemplateResponse("strength-weakness.html", {"request": request})

# 검색전에 존재 여부 확인
@app.get("/api/check-product/{product_id}")
async def check_product(product_id: str):
    """ ✅ 제품 ID 존재 여부 확인 API """
    conn = get_mysql_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database Connection Failed")
    
    try:
        with conn.cursor() as cursor:
            query = "SELECT COUNT(*) AS count FROM products WHERE product_id = %s"
            cursor.execute(query, (product_id,))
            result = cursor.fetchone()
        
        return {"exists": result["count"] > 0}  # ✅ 존재 여부 반환
    
    except pymysql.MySQLError as e:
        print(f"🔥 [DB Error] {e}")
        raise HTTPException(status_code=500, detail="Database Query Failed")
    
    finally:
        conn.close()



#--------------------------------------------------------#




#--------------------------------------------------------#
#이도훈 - 날짜 분석

from pydantic import BaseModel

class DateFilter(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    product_id: Optional[int] = None
    brand: Optional[str] = None

# 날짜 분석 페이지 렌더링
@app.get("/date_analysis.html")
async def dashboard(request: Request):
    print(f"🔹 요청 받음: {request}")  # 요청 객체 확인
    return templates.TemplateResponse("date_analysis.html", {"request": request})

# 브랜드 목록 API
@app.get("/api/brands")
async def get_brands():
    conn = None
    cursor = None
    
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()

        # 브랜드 목록 조회
        query = "SELECT DISTINCT brand FROM products ORDER BY brand"
        cursor.execute(query)
        result = cursor.fetchall()

        # 디버깅용 로그 출력
        print("🔍 브랜드 조회 결과:", result)

        # 브랜드 목록 반환
        brands = [row["brand"] for row in result]
        return {"brands": brands}

    except pymysql.Error as e:  # ✅ mysql.connector 예외 처리
        print(f"❌ 데이터베이스 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
            
    except Exception as e:
        print(f"❌ 브랜드 조회 오류: {e}")
        return {"error": "Failed to fetch brands"}, 500

    finally:  
        if cursor:
            cursor.close()
        if conn:
            conn.close()
# =====================================================================================================                
# 날짜 분석 API
@app.post("/api/date_analysis")
async def analyze_reviews(filter: DateFilter):
    print(f"✅ 날짜 분석 API 호출됨: {filter}")


    conn = get_mysql_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Failed to connect to the database")

    cursor = conn.cursor()

    # 조건에 따라 SQL 쿼리 분기 처리
    if filter.brand and filter.startDate and filter.endDate:
        # Case 1: 브랜드 + 날짜
        query = """
            SELECT DATE(r.review_date) AS date, COUNT(*) AS count 
            FROM review_analysis r
            JOIN products p ON r.product_id = p.product_id 
            WHERE p.brand = %s 
              AND review_date BETWEEN %s AND %s
              AND review_date BETWEEN '2017-06-01' AND '2025-01-31'
            GROUP BY DATE(r.review_date)
            ORDER BY date
        """
        params = [filter.brand, filter.startDate, filter.endDate]
    elif filter.brand:
        # Case 2: 브랜드 단독 (전체 기간)
        query = """
            SELECT DATE(r.review_date) AS date, COUNT(*) AS count 
            FROM review_analysis r
            JOIN products p ON r.product_id = p.product_id
            WHERE p.brand = %s
              AND review_date BETWEEN '2017-06-01' AND '2025-01-31'
            GROUP BY DATE(r.review_date)
            ORDER BY date
        """
        params = [filter.brand]


    elif filter.product_id and filter.startDate and filter.endDate:
        # 1. product_id, startDate, endDate 모두 입력된 경우
        query = """
            SELECT DATE(review_date) AS date, COUNT(*) AS count 
            FROM review_analysis 
            WHERE review_date BETWEEN %s AND %s
              AND product_id = %s
              AND review_date BETWEEN '2017-06-01' AND '2025-01-31'              
            GROUP BY DATE(review_date)
            ORDER BY date
        """
        params = [filter.startDate, filter.endDate, filter.product_id]
    elif filter.product_id:
        # 2. product_id만 입력된 경우 (전체 날짜)
        query = """
            SELECT DATE(review_date) AS date, COUNT(*) AS count 
            FROM review_analysis 
            WHERE product_id = %s
              AND review_date BETWEEN '2017-06-01' AND '2025-01-31'            
            GROUP BY DATE(review_date)
            ORDER BY date
        """
        params = [filter.product_id]
    elif filter.startDate and filter.endDate:
        # 3. startDate와 endDate만 입력된 경우 (모든 제품)
        query = """
            SELECT DATE(review_date) AS date, COUNT(*) AS count 
            FROM review_analysis 
            WHERE review_date BETWEEN %s AND %s
               AND review_date BETWEEN '2017-06-01' AND '2025-01-31'           
            GROUP BY DATE(review_date)
            ORDER BY date
        """
        params = [filter.startDate, filter.endDate]
    else:
        # 4. 아무 조건도 입력되지 않은 경우 에러 반환
        raise HTTPException(status_code=400, detail="브랜드/제품ID 또는 날짜 필수") 

    try:
        # 쿼리 출력
        print("🔍 실행할 SQL 쿼리:", query)
        print("📌 바인딩할 파라미터:", params)

        cursor.execute(query, params)
        result = cursor.fetchall()

        # 결과가 없을 경우
        if not result:
            return {"labels": [], "values": []}

        # 결과 반환
        return {
            "labels": [row["date"].strftime("%Y-%m-%d") for row in result],
            "values": [row["count"] for row in result]
        }

    except pymysql.ProgrammingError as e:
        print(f"❌ MySQL Programming Error: {e}")
        raise HTTPException(status_code=500, detail=f"MySQL Programming Error: {str(e)}")

    except pymysql.OperationalError as e:
        print(f"❌ MySQL Connection Error: {e}")
        raise HTTPException(status_code=500, detail=f"MySQL Connection Error: {str(e)}")

    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")

    finally:
        if cursor:  # ✅ 안전한 확인 방식
            cursor.close()
        if conn:
            conn.close()

from typing import Optional
from fastapi import Query, HTTPException
import pymysql

# 🔹 MySQL 연결 함수
def get_mysql_connection():
    try:
        return pymysql.connect(
            host="15.152.242.221",
            user="admin",
            password="Admin@1234",
            database="musinsa_pd_rv",
            cursorclass=pymysql.cursors.DictCursor
        )
    except pymysql.MySQLError as e:
        print(f"❌ MySQL Connection Error: {e}")
        return None

# ✅ 전체 판매량 API (리뷰 개수 기반)
@app.get("/api/total-sales")
async def total_sales(startDate: Optional[str] = None, endDate: Optional[str] = None):
    """전체 판매량 추이 데이터 (리뷰 개수 기반)"""
    conn = get_mysql_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()

    # 리뷰 개수 기반 판매량 조회 (전체 기간)
    query = """
        SELECT DATE(r.review_date) AS date, COUNT(*) AS sales
        FROM reviews r
        WHERE r.review_date BETWEEN '2017-06-01' AND '2025-01-31'
    """
    params = []

    # 날짜 필터 추가
    if startDate and endDate:
        query += " AND r.review_date BETWEEN %s AND %s"
        params.extend([startDate, endDate])

    query += " GROUP BY DATE(r.review_date) ORDER BY date"

    cursor.execute(query, params)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    # 🔹 결과 로그 출력
    print(f"📊 총 판매량 데이터: {results}")

    # 결과 반환
    return {
        "labels": [row["date"].strftime("%Y-%m-%d") if row["date"] else "N/A" for row in results],
        "values": [row["sales"] for row in results]
    }

# ✅ 카테고리별 판매량 API (리뷰 개수 기반)
@app.get("/api/category-sales")
async def category_sales(startDate: Optional[str] = None, endDate: Optional[str] = None):
    """카테고리별 판매량 추이 데이터 (리뷰 개수 기반)"""
    conn = get_mysql_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()

    # 리뷰 개수 기반 카테고리별 판매량 조회
    query = """
        SELECT DATE(r.review_date) AS date, p.category, COUNT(*) AS sales
        FROM reviews r
        JOIN products p ON r.product_id = p.product_id
        WHERE r.review_date BETWEEN '2017-06-01' AND '2025-01-31'
    """
    params = []

    # 날짜 필터 추가
    if startDate and endDate:
        query += " AND r.review_date BETWEEN %s AND %s"
        params.extend([startDate, endDate])

    query += " GROUP BY DATE(r.review_date), p.category ORDER BY date"

    cursor.execute(query, params)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    # 🔹 결과 로그 출력
    print(f"📊 카테고리별 판매량 데이터: {results}")

    # 데이터 가공 (날짜별 카테고리 매핑)
    category_sales_data = {}
    for row in results:
        date = row["date"].strftime("%Y-%m-%d") if row["date"] else "N/A"
        category = row["category"] if row["category"] else "기타"
        sales = row["sales"]

        if date not in category_sales_data:
            category_sales_data[date] = {}

        category_sales_data[date][category] = sales

    return category_sales_data

# ✅ 성별 판매량 API (리뷰 개수 기반)
@app.get("/api/gender-sales")
async def gender_sales(startDate: Optional[str] = None, endDate: Optional[str] = None):
    """성별 판매량 추이 데이터 (리뷰 개수 기반)"""
    conn = get_mysql_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()

    # 리뷰 개수 기반 성별별 판매량 조회
    query = """
        SELECT DATE(r.review_date) AS date, p.gender, COUNT(*) AS sales
        FROM reviews r
        JOIN products p ON r.product_id = p.product_id
        WHERE r.review_date BETWEEN '2017-06-01' AND '2025-01-31'
    """
    params = []

    # 날짜 필터 추가
    if startDate and endDate:
        query += " AND r.review_date BETWEEN %s AND %s"
        params.extend([startDate, endDate])

    query += " GROUP BY DATE(r.review_date), p.gender ORDER BY date"

    cursor.execute(query, params)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    # 🔹 결과 로그 출력
    print(f"📊 성별 판매량 데이터: {results}")

    # 데이터 가공 (날짜별 성별 매핑)
    gender_sales_data = {}
    for row in results:
        date = row["date"].strftime("%Y-%m-%d") if row["date"] else "N/A"
        gender = row["gender"] if row["gender"] else "기타"
        sales = row["sales"]

        if date not in gender_sales_data:
            gender_sales_data[date] = {}

        gender_sales_data[date][gender] = sales

    return gender_sales_data


#--------------------------------------------------------#
#강대웅 - 이상감지 작업
import time
import traceback
import asyncio
import pymysql
from datetime import datetime, date
from decimal import Decimal
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException, Query
from starlette.websockets import WebSocketState
from collections import defaultdict


# 🔹 Redis 설정
REDIS_HOST = "15.152.242.221"
REDIS_PORT = 6379

# 🔹 WebSocket & 캐싱 설정
ANOMALY_CACHE = None
LAST_CACHE_TIME = 0
active_connections = set()  # ✅ 단순한 set()으로 변경
MAX_CONNECTIONS = 10
redis_client = None  # ✅ 전역 Redis 연결 변수

async def get_redis():
    """ Redis 연결을 한 번만 생성하고 재사용 """
    global redis_client
    if redis_client is None:
        redis_client = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")
    return redis_client



# ✅ WebSocket 연결 관리
async def websocket_manager():
    """ WebSocket 연결 감지 및 정리 """
    while True:
        await asyncio.sleep(5)  # 🔥 불필요한 반복 방지
        disconnected_clients = set()
        for conn in active_connections.copy():
            try:
                await conn.send_json({"ping": "keep-alive"})
            except WebSocketDisconnect:
                disconnected_clients.add(conn)

        # 🔥 연결 해제된 클라이언트 삭제
        for client in disconnected_clients:
            active_connections.discard(client)

        print(f"✅ 현재 활성 WebSocket 수: {len(active_connections)}")

async def websocket_handler(websocket: WebSocket):
    """ WebSocket을 통해 Redis Pub/Sub 메시지를 수신 및 전달 """
    await websocket.accept()
    active_connections.add(websocket)

    redis = await get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe("review_updates")  # ✅ WebSocket이 Redis Pub/Sub 채널 구독

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=10)
            if message and message["data"]:
                try:
                    alert_data = json.loads(message["data"].decode("utf-8"))  # ✅ JSON 데이터 변환
                    await websocket.send_json(alert_data)  # ✅ WebSocket으로 클라이언트에 메시지 전송
                    print(f"✅ WebSocket 알림 전송 성공: {alert_data}")
                except Exception as e:
                    print(f"❌ WebSocket 메시지 변환 오류: {e}")
            await asyncio.sleep(1)  # ✅ 불필요한 반복 최소화
    except WebSocketDisconnect:
        print("🔌 WebSocket 연결이 끊어졌습니다.")
    finally:
        active_connections.discard(websocket)  # ✅ 연결 끊긴 클라이언트 제거
        await pubsub.unsubscribe("review_updates")  # ✅ WebSocket 연결 종료 시 Redis 구독 해제

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    if len(active_connections) >= MAX_CONNECTIONS:
        print("⚠️ WebSocket 최대 연결 수 초과. 연결 거부됨.")
        await websocket.close()
        return

    print("🔗 WebSocket 클라이언트 연결 요청됨.")
    await websocket.accept()
    active_connections.add(websocket)
    print(f"✅ WebSocket 연결됨 (총 {len(active_connections)}개)")

    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_json({"ping": "keep-alive"})  
            try:
                message = await asyncio.wait_for(websocket.receive_text(), timeout=60)  # ⏳ 60초로 증가
                print(f"📩 WebSocket 메시지 수신: {message}")
            except asyncio.TimeoutError:
                print("⏳ 클라이언트 메시지 없음 (60초 경과)")
            await asyncio.sleep(30)  # ✅ 30초마다 ping 메시지 전송
    except WebSocketDisconnect:
        print("🔌 WebSocket 연결 해제됨.")
    except Exception as e:
        print(f"🔥 WebSocket 오류 발생: {e}")
    finally:
        if websocket in active_connections:
            active_connections.discard(websocket)
        print(f"⚠️ WebSocket 클라이언트 연결 해제됨 (총 {len(active_connections)}개 남음)")



# ✅ 부정 리뷰 감지 후 WebSocket 알림
async def send_negative_review_alert(review):
    """ WebSocket을 통해 이상 감지 리뷰 발생 알림 전송 (이상 감지 유형 추가) """
    redis = await get_redis()

    # ✅ 이미 보낸 알림인지 Redis에서 확인
    redis_key = f"{review['review_id']}:{review['product_id']}:{review['category']}"
    if await redis.hexists("sent_alerts", redis_key):
        print(f"⚠️ WebSocket 중복 방지: 리뷰 ID {review['review_id']} ({review['category']})는 이미 전송됨.")
        return  # 이미 보낸 알림이면 종료

    # ✅ 이상 감지 유형 판단
    if review["sentiment"] == 0:
        anomaly_type = "부정 리뷰 감지 (모든 평점 포함)"  
    elif review["rating"] in (1.0, 2.0, 3.0):
        anomaly_type = "평점이 1~3점인 리뷰입니다!"  
    elif review["sentiment"] == 1 and review["rating"] in (1.0, 2.0, 3.0):
        anomaly_type = "중립 감성이지만 평점이 낮음!"
    else:
        print(f"ℹ️ WebSocket 알림 제외됨: 리뷰 ID {review['review_id']} (평점 {review['rating']}, 감성 {review['sentiment']})")
        return  # ✅ 감성 분석이 부정이 아니고 1~3점도 아닌 경우 알림 제외

    alert_message = {
        "alert": f"📢 이상 감지: {anomaly_type}",
        "review": {
            "review_id": review["review_id"],
            "product_id": review["product_id"],
            "product_name": review.get("product_name", "상품명 없음"),
            "brand": review.get("brand", "브랜드 없음"),
            "price": review.get("price", "가격 정보 없음"),
            "image_url": review.get("image_url", ""),
            "category": review.get("category", "미분류"),
            "sentence": review.get("sentence", "리뷰 내용 없음"),
            "rating": review["rating"],
            "sentiment": review["sentiment"],
            "type": anomaly_type,  
            "review_date": review.get("review_date", "날짜 없음")
        }
    }

    # ✅ Redis Pub/Sub을 통해 WebSocket으로 알림 발행 (로그 추가)
    print(f"📢 [send_negative_review_alert] Redis PUBLISH 실행 전: {alert_message}")
    await redis.publish("review_updates", json.dumps(alert_message))
    print(f"📢 [send_negative_review_alert] Redis PUBLISH 실행 완료 🚀")

    await asyncio.sleep(2)  

    # ✅ WebSocket 메시지를 병렬 전송 (성능 최적화)
    disconnected_clients = set()
    send_tasks = []
    for conn in active_connections.copy():
        try:
            send_tasks.append(conn.send_json(alert_message))
        except WebSocketDisconnect:
            disconnected_clients.add(conn)
        except Exception as e:
            print(f"⚠️ WebSocket 메시지 전송 오류: {e}")
            disconnected_clients.add(conn)

    if send_tasks:
        await asyncio.gather(*send_tasks, return_exceptions=True)

    for client in disconnected_clients:
        active_connections.discard(client)

    print(f"✅ WebSocket으로 {len(active_connections)}개 클라이언트에게 알림 전송 완료")

    # ✅ WebSocket 알림이 정상적으로 전송되었으면 Redis에 기록
    await redis.publish("review_updates", json.dumps(alert_message))
    await redis.hset("sent_alerts", redis_key, "sent")
    await redis.expire("sent_alerts", 3600)  

    print(f"✅ WebSocket으로 {len(active_connections)}개 클라이언트에게 알림 전송 완료")


# ✅ 최신 부정 리뷰 저장 및 알림
import json

async def save_negative_review(review):
    """ 중복 저장을 방지하고, 최대 100개의 리뷰만 유지 """
    redis = await get_redis()
    
    try:
        review_id = review.get("review_id")
        category = review.get("category", "미분류")

        # 🔹 중복 저장 방지 (review_id:category 형식 사용)
        redis_key = f"{review_id}:{category}"

        if await redis.hexists("negative_reviews_map", redis_key):
            print(f"⚠️ 중복 리뷰 저장 방지: {redis_key}")
            return  # 중복된 리뷰는 저장하지 않음 (WebSocket 알림도 X)

        json_review = json.dumps(review)

        # ✅ Redis에 저장 (review_id:category 형식)
        await redis.hset("negative_reviews_map", redis_key, json_review)
        await redis.lpush("negative_reviews", json_review)

        # ✅ Redis Pub/Sub에 직접 메시지 발행 (WebSocket이 감지할 수 있도록) 🚀
        await redis.publish("review_updates", json_review)
        print(f"📢 [Redis PUBLISH] 부정 리뷰 메시지 전송: {json_review}")

        # ✅ WebSocket 알림 실행
        await send_negative_review_alert(review)

        # 🔹 최대 개수 유지 (100개 초과 시 가장 오래된 데이터 삭제)
        max_reviews = 100
        current_length = await redis.llen("negative_reviews")
        if current_length > max_reviews:
            oldest_review = await redis.rpop("negative_reviews")
            if oldest_review:
                oldest_review_data = json.loads(oldest_review.decode())
                oldest_review_key = f"{oldest_review_data['review_id']}:{oldest_review_data.get('category', '미분류')}"
                await redis.hdel("negative_reviews_map", oldest_review_key)  # 해시맵에서도 삭제

    except Exception as e:
        print(f"❌ Redis 저장 오류: {e}")



@app.get("/api/anomaly-products/{year}/{month}")
async def get_anomaly_products(year: int, month: int):
    """특정 달에 이상 감지된 제품 중 상위 10개 조회"""
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month+1:02d}-01"

    query = """
        SELECT p.product_id, p.product_name, COUNT(*) AS anomaly_count
        FROM review_analysis r
        JOIN products p ON r.product_id = p.product_id
        WHERE r.review_date >= %s AND r.review_date < %s
        AND (r.sentiment = 0 OR r.rating IN (1, 2, 3))
        GROUP BY p.product_id, p.product_name
        ORDER BY anomaly_count DESC
        LIMIT 10;
    """

    try:
        pool = await get_mysql_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                print(f"📡 [API 호출] {year}-{month}월 이상 감지 제품 조회 실행")
                await cursor.execute(query, (start_date, end_date))
                results = await cursor.fetchall()

        if not results:
            return {"error": "No Data", "message": f"{year}년 {month}월에 이상 감지된 제품이 없습니다."}

        return [{"product_id": row["product_id"], "product_name": row["product_name"], "anomaly_count": row["anomaly_count"]} for row in results]

    except Exception as e:
        print(f"🔥 [API 오류] {e}")
        return {"error": "Internal Server Error", "detail": str(e)}

@app.get("/api/negative-reviews")
async def get_negative_reviews():
    redis = await get_redis()
    reviews = await redis.lrange("negative_reviews", 0, -1)

    print(f"✅ Redis에서 불러온 부정 리뷰 개수: {len(reviews)}")  # 추가!

    cleaned_reviews = []
    for review in reviews:
        try:
            decoded_review = review.decode()
            if not decoded_review.startswith("{"):
                decoded_review = decoded_review.replace("'", '"')
            cleaned_reviews.append(json.loads(decoded_review))
        except json.JSONDecodeError as e:
            print(f"❌ JSON 디코딩 오류: {e}")
            continue  

    return cleaned_reviews


# ✅ 특정 부정 리뷰 삭제 API
@app.delete("/api/negative-reviews/{review_id}")
async def delete_negative_review(review_id: str):
    """ Redis에서 특정 부정 리뷰 삭제 """
    redis = await get_redis()
    reviews = await redis.lrange("negative_reviews", 0, -1)
    
    for review in reviews:
        try:
            review_data = json.loads(review.decode())
            if review_data.get("review_id") == review_id:
                await redis.lrem("negative_reviews", 1, review)
                return {"message": f"부정 리뷰 {review_id} 삭제 완료"}
        except json.JSONDecodeError as e:
            print(f"❌ JSON 디코딩 오류: {e}")
    
    raise HTTPException(status_code=404, detail="해당 부정 리뷰를 찾을 수 없습니다.")


# ✅ JSON 변환 함수 (Decimal, date 변환)
def json_serial(obj):
    """Decimal 및 date 타입을 JSON 변환 가능하게 변경"""
    if isinstance(obj, Decimal):
        return float(round(obj, 2))  # ✅ 소수점 2자리까지 반올림
    elif isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, list):
        return [json_serial(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: json_serial(value) for key, value in obj.items()}
    return obj



from datetime import timedelta

@app.get("/api/anomaly-summary")
async def anomaly_summary():
    """오늘 및 이번 주의 부정 리뷰 및 이상 감지 리뷰 분석"""
    try:
        print("✅ [anomaly_summary] API 호출됨")
        conn = get_mysql_connection()
        cursor = conn.cursor()

        today = datetime.today().strftime("%Y%m%d")
        today_table = f"today_reviews_{today}_analysis"
        print(f"🔹 [MySQL] 오늘 테이블: {today_table}")

        today_date = datetime.today().date()
        monday_date = today_date - timedelta(days=today_date.weekday())
        print(f"🔹 [MySQL] 이번 주 시작일: {monday_date}")

        cursor.execute(f"SHOW TABLES LIKE '{today_table}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=500, detail=f"❌ 테이블 {today_table}이 존재하지 않습니다.")

        # ✅ 오늘 감성 분석
        cursor.execute(f"""
            SELECT 
                SUM(sentiment = 0) AS negative_reviews,
                SUM(sentiment = 1) AS neutral_reviews,
                SUM(sentiment = 2) AS positive_reviews
            FROM {today_table}
        """)
        row = cursor.fetchone() or {}
        today_sentiment_counts = {
            0: int(row.get("negative_reviews", 0)),
            1: int(row.get("neutral_reviews", 0)),
            2: int(row.get("positive_reviews", 0))
        }

        print(f"📊 오늘 감성 분석 결과: {today_sentiment_counts}")

        # ✅ 오늘 평점 분포 분석
        cursor.execute(f"SELECT rating, COUNT(*) AS count FROM {today_table} GROUP BY rating")
        today_rating_distribution = {
            int(row["rating"]): int(row.get("count", 0))
            for row in cursor.fetchall() if row["rating"] is not None
        }
        print(f"📊 오늘 평점 분포: {today_rating_distribution}")

        # ✅ 오늘 평균 평점 계산
        cursor.execute(f"SELECT AVG(rating) AS avg_rating FROM {today_table}")
        avg_rating_result = cursor.fetchone()
        today_avg_rating = avg_rating_result["avg_rating"] if avg_rating_result and avg_rating_result["avg_rating"] else 0
        print(f"📌 오늘 평균 평점: {today_avg_rating:.2f}")

        # ✅ 오늘 카테고리별 리뷰 개수 분석 (복합 카테고리 개별 분리)
        cursor.execute(f"""
            SELECT sub.category, COUNT(*) as count
            FROM (
                SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(category, ',', numbers.n), ',', -1)) AS category
                FROM {today_table}
                JOIN (
                    SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                ) numbers ON CHAR_LENGTH(category) - CHAR_LENGTH(REPLACE(category, ',', '')) >= numbers.n - 1
            ) AS sub
            WHERE sub.category IS NOT NULL AND sub.category <> ''
            GROUP BY sub.category;
        """)
        today_category_counts = {row["category"]: row["count"] for row in cursor.fetchall()}
        print(f"📊 오늘 카테고리 분석 (수정됨): {today_category_counts}")

        cursor.execute(f"""
            SELECT sub.category, COUNT(*) as count
            FROM (
                SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(category, ',', numbers.n), ',', -1)) AS category
                FROM {today_table}
                JOIN (
                    SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                ) numbers ON CHAR_LENGTH(category) - CHAR_LENGTH(REPLACE(category, ',', '')) >= numbers.n - 1
                WHERE sentiment = 0  -- ✅ 필터를 서브쿼리 내부가 아닌 여기에 위치
            ) AS sub
            WHERE sub.category IS NOT NULL AND sub.category <> ''
            GROUP BY sub.category;
        """)
        today_negative_category_counts = {row["category"]: row["count"] for row in cursor.fetchall()}
        print(f"📊 오늘 부정 리뷰 카테고리 분석 (수정됨): {today_negative_category_counts}")

        # ✅ 오늘 부정 리뷰 감지
        cursor.execute(f"""
            SELECT review_id, product_id, sentence, category, review_date, rating, sentiment
            FROM {today_table} WHERE sentiment = 0 ORDER BY review_date DESC
        """)
        today_negative_reviews = cursor.fetchall() or []
        print(f"📌 오늘 부정 리뷰 개수: {len(today_negative_reviews)}")

        # ✅ 오늘 이상 감지 리뷰 감지 (수정된 조건)
        cursor.execute(f"""
            SELECT r.review_id, r.product_id, p.product_name, p.brand, p.price, p.image_url,
                r.sentence, r.category, r.review_date, r.rating, r.sentiment,
                rv.review_size  -- ✅ 추가
            FROM {today_table} r
            JOIN products p ON r.product_id = p.product_id
            LEFT JOIN reviews rv ON r.review_id = rv.review_id  -- ✅ 추가
            WHERE r.sentiment = 0 OR r.rating IN (1.0, 2.0, 3.0)
            ORDER BY r.review_date DESC;
        """)
        today_anomaly_reviews = cursor.fetchall()
        print(f"⚠️ 오늘 이상 감지 리뷰 개수: {len(today_anomaly_reviews)}")


        # ✅ Redis 저장 및 WebSocket 알림 추가 (중복 검사를 먼저 수행)
        redis = await get_redis()  # ✅ Redis 연결 가져오기

        for review in today_anomaly_reviews:
            review_id = review["review_id"]
            review_data = {
                "review_id": review["review_id"],
                "product_id": review["product_id"],
                "sentence": review["sentence"],
                "category": review["category"],
                "review_date": json_serial(review["review_date"]),
                "rating": review["rating"],
                "sentiment": review["sentiment"],
                "review_size": review.get("review_size", "미지정")  # ✅ 추가
            }

            # ✅ 중복된 리뷰인지 Redis에서 확인
            # ✅ 리뷰 ID와 카테고리 조합으로 중복 여부 체크
            redis_key = f"{review_id}:{review['category']}"  # review_id + category 조합으로 중복 방지

            if await redis.hexists("negative_reviews_map", redis_key):
                print(f"🔄 이미 저장된 부정 리뷰: {redis_key}, WebSocket 알림 전송 방지")
                continue  # 이미 저장된 리뷰는 저장 및 알림을 보내지 않음

            # ✅ Redis에 저장
            await redis.hset("negative_reviews_map", redis_key, json.dumps(review_data))

            print(f"✅ Redis 저장 시도: {review_data}")  # ✅ 디버깅 로그 추가

            try:
                # ✅ `save_negative_review()` 실행 (중복 저장 방지 후 저장 및 WebSocket 알림)
                await save_negative_review(review_data)
            except Exception as e:
                print(f"❌ Redis 저장 실패: {e}")  # ✅ Redis 저장 실패 시 로그 추가
                traceback.print_exc()




        # ✅ 이번 주 데이터 가져오기 (이번 주 월요일 ~ 오늘)
        weekly_sentiment_counts = {0: 0, 1: 0, 2: 0}
        weekly_category_counts = {}
        weekly_negative_category_counts = {}
        weekly_rating_distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        weekly_avg_rating_sum = 0
        weekly_avg_rating_count = 0
        weekly_anomaly_reviews = []  # ✅ 루프 바깥에서 초기화하여 누적 저장되도록 변경

        for i in range(7):  # ✅ 월요일(03/03)부터 일요일(03/09)까지 7일간 조회
            day = monday_date + timedelta(days=i)
            day_table = f"today_reviews_{day.strftime('%Y%m%d')}_analysis"

            cursor.execute(f"SHOW TABLES LIKE '{day_table}'")
            if not cursor.fetchone():
                print(f"⚠️ {day_table} 테이블이 존재하지 않음, 건너뜀.")
                continue  # ✅ 테이블이 없으면 해당 날짜는 건너뛰기

            print(f"✅ [DEBUG] {day_table} 테이블 존재 확인됨.")

            # ✅ 이번 주 감성 분석
            cursor.execute(f"""
                SELECT 
                    SUM(sentiment = 0) AS negative_reviews,
                    SUM(sentiment = 1) AS neutral_reviews,
                    SUM(sentiment = 2) AS positive_reviews
                FROM {day_table}
            """)
            row = cursor.fetchone()
            weekly_sentiment_counts[0] += row["negative_reviews"] or 0
            weekly_sentiment_counts[1] += row["neutral_reviews"] or 0
            weekly_sentiment_counts[2] += row["positive_reviews"] or 0

            # ✅ 이번 주 평점 분포 분석
            cursor.execute(f"""
                SELECT rating, COUNT(*) AS count
                FROM {day_table}
                GROUP BY rating
            """)
            for row in cursor.fetchall():
                weekly_rating_distribution[row["rating"]] += row["count"]

            # ✅ 이번 주 평균 평점 계산
            cursor.execute(f"SELECT AVG(rating) AS avg_rating FROM {day_table}")
            avg_rating = cursor.fetchone()["avg_rating"]
            if avg_rating:
                weekly_avg_rating_sum += avg_rating
                weekly_avg_rating_count += 1

            # ✅ 이번 주 카테고리별 리뷰 개수 분석
            cursor.execute(f"SELECT category FROM {day_table}")
            for row in cursor.fetchall():
                categories = row.get("category", "").split(",")  # ✅ 안전하게 가져오기
                for cat in categories:
                    cat = cat.strip()
                    if cat:
                        weekly_category_counts[cat] = weekly_category_counts.get(cat, 0) + 1
            print("📊 이번 주 전체 카테고리 데이터:", weekly_category_counts)  # ✅ 디버깅

            # ✅ 이번 주 부정 리뷰 카테고리별 개수 분석
            cursor.execute(f"SELECT category FROM {day_table} WHERE sentiment = 0")
            for row in cursor.fetchall():
                categories = row.get("category", "").split(",")  # ✅ 안전하게 가져오기
                for cat in categories:
                    cat = cat.strip()
                    if cat:
                        weekly_negative_category_counts[cat] = weekly_negative_category_counts.get(cat, 0) + 1
            print("📊 이번 주 부정 카테고리 데이터:", weekly_negative_category_counts)  # ✅ 디버깅

            # ✅ 이번 주 이상 감지 리뷰 목록 (변수 초기화 제거)
            cursor.execute(f"""
                SELECT r.review_id, r.product_id, p.product_name, p.brand, p.price, p.image_url,
                    r.sentence, r.category, r.review_date, r.rating, r.sentiment,
                    rv.review_size  # ✅ review_size 추가 (JOIN)
                FROM {day_table} r
                JOIN products p ON r.product_id = p.product_id
                LEFT JOIN reviews rv ON r.review_id = rv.review_id  # ✅ reviews 테이블에서 review_size 가져오기
                WHERE r.sentiment = 0  -- ✅ 모든 부정 리뷰 포함 (rating 관계없이)
                OR r.rating IN (1.0, 2.0, 3.0)  -- ✅ 1~3점 리뷰도 포함
                ORDER BY r.review_date DESC
                    """)
            weekly_anomaly_reviews.extend(cursor.fetchall())  # ✅ 매 루프마다 누적 저장

        weekly_avg_rating = weekly_avg_rating_sum / weekly_avg_rating_count if weekly_avg_rating_count else 0

        cursor.close()
        conn.close()

        # ✅ JSON 변환하여 반환
        response_data = {
            "today": {
                "sentiment_analysis": today_sentiment_counts,
                "category_analysis": today_category_counts,
                "negative_category_analysis": today_negative_category_counts,
                "negative_reviews": today_negative_reviews,
                "anomaly_reviews": today_anomaly_reviews,
                "rating_distribution": today_rating_distribution,
                "avg_rating": today_avg_rating
            },
            "weekly": {
                "sentiment_analysis": weekly_sentiment_counts,
                "category_analysis": weekly_category_counts,
                "negative_category_analysis": weekly_negative_category_counts,
                "rating_distribution": weekly_rating_distribution,
                "avg_rating": weekly_avg_rating,
                "anomaly_reviews": weekly_anomaly_reviews  # ✅ 추가!
            }
        }

        return json.loads(json.dumps(response_data, default=json_serial))

    except Exception as e:
        print(f"🔥 API 오류 발생: {str(e)}")  # ✅ 추가 로그 출력
        traceback.print_exc()  # ✅ 자세한 오류 출력
        raise HTTPException(status_code=500, detail=f"예상치 못한 오류: {str(e)}")



# ✅ Redis Pub/Sub 리스너 (디버깅 추가)
async def redis_listener():
    redis = await get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe("review_updates")
    print("✅ Redis Pub/Sub 'review_updates' 채널 구독 시작")

    while True:
        try:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=10)
            if message and message["type"] == "message":
                data = message["data"]

                try:
                    decoded_data = data.decode("utf-8")
                    parsed_data = json.loads(decoded_data)
                    print(f"📢 [Redis] 메시지 수신 성공: {parsed_data}")  # ✅ 여기서 메시지가 보이는지 확인!

                    # ✅ WebSocket으로 메시지 전송
                    disconnected_clients = set()
                    for conn in active_connections.copy():
                        try:
                            await conn.send_json(parsed_data)
                        except WebSocketDisconnect:
                            disconnected_clients.add(conn)
                        except Exception as e:
                            print(f"🚨 WebSocket 메시지 전송 오류: {e}")

                    for client in disconnected_clients:
                        active_connections.discard(client)

                except json.JSONDecodeError as json_error:
                    print(f"🚨 JSON 파싱 오류 발생: {json_error}, 원본 데이터: {data}")

            else:
                print("⏳ Redis Pub/Sub 대기 중...")

            await asyncio.sleep(5)

        except Exception as e:
            print(f"❌ Redis Pub/Sub 오류 발생: {e}")

# ✅ 중복 감지된 리뷰를 Redis에서 삭제하는 코드 추가
async def reset_sent_alerts():
    redis = await get_redis()
    await redis.delete("sent_alerts")  # 모든 기존 알림 삭제
    print("🔄 Redis `sent_alerts` 초기화 완료!")

# ✅ MySQL 변경 감지 후 Redis Pub/Sub 메시지 전송
# ✅ MySQL 변경 감지 후 Redis Pub/Sub 메시지 전송 (수정됨)
async def check_table_updates_task():
    """ 매일 변경되는 테이블을 감지하여 새로운 리뷰만 감지하고 WebSocket 알림 전송 """
    redis = await get_redis()

    while True:
        await asyncio.sleep(10)  # ✅ 10초마다 실행

        today = datetime.today().strftime("%Y%m%d")
        today_table = f"today_reviews_{today}_analysis"

        conn = get_mysql_connection()
        cursor = conn.cursor()

        # 🔹 테이블 존재 여부 확인
        cursor.execute(f"SHOW TABLES LIKE '{today_table}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            print(f"⏳ {today_table} 테이블이 아직 생성되지 않음. 5분 후 다시 확인")
            cursor.close()
            conn.close()
            await asyncio.sleep(300)
            continue

        # 🔹 Redis에서 마지막 감지된 리뷰 ID 목록 가져오기 (여러 개 가능)
        last_review_ids = await redis.lrange("last_review_ids", 0, -1)
        last_review_ids = [id.decode() for id in last_review_ids] if last_review_ids else []

        # 🔹 가장 최신 이상 감지 리뷰 목록 가져오기 (sentiment = 0 또는 rating 1~3)
        cursor.execute(f"""
            SELECT review_id, product_id, sentence, category, review_date, rating, sentiment
            FROM {today_table}
            WHERE sentiment = 0 OR rating IN (1.0, 2.0, 3.0)
            ORDER BY review_date DESC
        """)
        latest_reviews = cursor.fetchall()  # ✅ 모든 이상 감지 리뷰 가져오기

        if not latest_reviews:
            print("⏳ 감지된 이상 리뷰 없음.")
            cursor.close()
            conn.close()
            continue  

        for review in latest_reviews:
            new_review_id = str(review["review_id"])

            # ✅ Redis에서 중복 체크 추가 (이미 저장된 리뷰면 무시)
            if new_review_id in last_review_ids:
                print(f"🔄 이미 처리된 리뷰입니다: {new_review_id}")
                continue

            print(f"📢 새로운 이상 감지 리뷰 발견! 리뷰 ID: {new_review_id}")

            review_data = {
                "review_id": review["review_id"],
                "product_id": review["product_id"],
                "sentence": review["sentence"],
                "category": review["category"],
                "review_date": json_serial(review["review_date"]),
                "rating": review["rating"],
                "sentiment": review["sentiment"]
            }

            # ✅ 이상 감지된 리뷰만 WebSocket 알림 전송
            print(f"⚠️ 이상 감지 리뷰 알림 발송: {review_data}")
            await send_negative_review_alert(review_data)

            # 🔹 Redis에 최신 리뷰 ID 저장 (중복 감지 방지, 여러 개 관리)
            await redis.lpush("last_review_ids", new_review_id)
            await redis.ltrim("last_review_ids", 0, 99)  # ✅ 최대 100개만 저장 (이전 리뷰 ID 관리)

        cursor.close()
        conn.close()

# ✅ MySQL 비동기 연결 풀 생성
async def get_mysql_pool():
    return await aiomysql.create_pool(
        host="15.152.242.221",
        user="admin",
        password="Admin@1234",
        db="musinsa_pd_rv",
        cursorclass=aiomysql.DictCursor,
        autocommit=True
    )

@app.get("/api/anomaly-shoes-category/{year}/{month}")
async def get_anomaly_shoes_category(year: int, month: int):
    """신발 카테고리별 이상 감지 리뷰 발생 빈도 조회"""
    try:
        # ✅ 시작 날짜: 해당 월 1일
        # response = await some_function_to_fetch_data(year, month)
        # headers = {"Cache-Control": "max-age=600"}  # 10분 동안 캐시 유지
        # return JSONResponse(content=response, headers=headers)
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month+1:02d}-01"

        # ✅ 신발 관련 카테고리 목록
        shoe_categories = ["구두", "등산화", "부츠/워커", "샌들/슬리퍼",
                           "스포츠화", "캔버스/단화", "트래킹화", "힐"]

        # ✅ SQL에서 카테고리 필터를 직접 적용
        category_filter = ", ".join(f"'{cat}'" for cat in shoe_categories)

        query = f"""
            SELECT p.category, COUNT(*) AS anomaly_count
            FROM review_analysis r
            JOIN products p ON r.product_id = p.product_id
            WHERE r.review_date >= %s AND r.review_date < %s  -- ✅ 다음 달 1일 미포함
            AND (r.sentiment = 0 OR r.rating IN (1, 2, 3))
            AND p.category IN ({category_filter})  -- ✅ 신발 카테고리 필터링
            GROUP BY p.category
            ORDER BY anomaly_count DESC;
        """

        async with await get_mysql_pool() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query, (start_date, end_date))  # ✅ start_date, end_date만 바인딩
                    results = await cursor.fetchall()

        print(f"🔍 SQL 조회 결과: {results}")  # ✅ FastAPI 로그에서 데이터 확인

        if not results:
            return {"error": "No Data", "message": f"{year}년 {month}월 신발 카테고리 데이터가 없습니다."}

        return {"data": [{"category": row["category"], "anomaly_count": row["anomaly_count"]} for row in results]}

    except Exception as e:
        import traceback
        error_message = traceback.format_exc()  # 예외 발생 위치까지 포함하여 상세 정보 추출
        print(f"🔥 [API 오류 발생]: {error_message}")  # 콘솔 로그 출력
        return {"error": "Internal Server Error", "detail": error_message}  # API 응답에 오류 상세 정보 포함





@app.get("/api/anomaly-trend/{year}/{month}")
async def get_anomaly_trend(year: int, month: int):
    start_date = f"{year}-{month:02d}-01"

    query = """
        SELECT DATE(review_date) AS review_date,  
            SUM(sentiment = 0) AS negative_count,
            SUM(rating IN (1.0, 2.0, 3.0)) AS low_rating_count
        FROM review_analysis
        WHERE DATE(review_date) BETWEEN %s AND LAST_DAY(%s)
        AND DATE(review_date) != '2025-03-04'  -- ✅ 특정 날짜 제외
        GROUP BY DATE(review_date)
        ORDER BY review_date;
    """

    try:
        pool = await get_mysql_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, (start_date, start_date))
                results = await cursor.fetchall()

        if not results:
            return {"error": "No Data", "message": f"{year}년 {month}월 데이터가 없습니다."}

        return [
            {
                "date": row["review_date"].strftime("%Y-%m-%d"),
                "negative_count": row["negative_count"],
                "low_rating_count": row["low_rating_count"]
            }
            for row in results
        ]

    except Exception as e:
        print(f"🔥 API 오류 발생: {e}")
        return {"error": "Internal Server Error", "detail": str(e)}





async def check_redis_negative_reviews():
    redis = await aioredis.from_url("redis://15.152.242.221:6379")
    
    # 저장된 부정 리뷰 해시맵 확인
    negative_reviews = await redis.hgetall("negative_reviews_map")
    
    # 출력
    print("📌 Redis에 저장된 부정 리뷰:")
    for key, value in negative_reviews.items():
        try:
            review = json.loads(value.decode()) if value else None
            if review:
                print(f"🔹 리뷰 ID: {review['review_id']}, 평점: {review['rating']}, 감성: {review['sentiment']}, 문장: {review['sentence']}")
        except Exception as e:
            print(f"❌ JSON 디코딩 오류: {e}")

async def main():
    await check_redis_negative_reviews()

# if __name__ == "__main__":
#     asyncio.run(main())  # ✅ FastAPI 실행 전에 실행할 경우에만 사용

from collections import Counter
from fastapi import HTTPException
from openai import AsyncOpenAI

@app.get("/api/anomaly-guideline/{product_id}")
async def generate_guideline_for_product(product_id: int):
    redis = await get_redis()
    cache_key = f"guideline:product:{product_id}"

    cached = await redis.get(cache_key)
    if cached:
        return {"guideline": cached}

    try:
        summary = await anomaly_summary()
        reviews = summary.get("weekly", {}).get("anomaly_reviews", [])
        product_reviews = [
            r for r in reviews
            if str(r.get("product_id")) == str(product_id)
        ]
    except Exception:
        raise HTTPException(status_code=500, detail="anomaly-summary 호출 실패")

    if not product_reviews:
        return {"guideline": f"ID {product_id}에 대한 이상 리뷰가 없어 가이드라인을 생성할 수 없습니다."}

    # ✅ 감성 라벨 변환 함수
    def get_sentiment_label(code):
        return {0: "부정", 1: "중립", 2: "긍정"}.get(code, "알 수 없음")

    # ✅ 감성 통계 요약
    sentiment_counter = Counter(r['sentiment'] for r in product_reviews)
    sentiment_summary = f"부정: {sentiment_counter.get(0, 0)}개, 중립: {sentiment_counter.get(1, 0)}개, 긍정: {sentiment_counter.get(2, 0)}개"

    # ✅ 평균 평점 계산
    avg_rating = sum(r["rating"] for r in product_reviews if r.get("rating")) / len(product_reviews)

    # ✅ 주요 카테고리 분석
    category_counter = Counter(
        c.strip() for r in product_reviews for c in r.get("category", "").split(",") if c.strip()
    )
    top_categories = ", ".join(f"{cat}({cnt}회)" for cat, cnt in category_counter.most_common(5))

    # ✅ 리뷰 샘플 모두 포함 (너무 많을 경우 [:30]로 제한해도 됨)
    sample_reviews = "\n".join([
        f'- "{r["sentence"]}" (평점: {r["rating"]}, 감성: {get_sentiment_label(r["sentiment"])})'
        for r in product_reviews
    ])
    product_name = product_reviews[0].get("product_name", "제품명 없음")  # 첫 리뷰에서 추출
    # ✅ GPT 프롬프트 구성 (❗중요: 평점 ≠ 감성 지시 포함)
    prompt = f"""
    [상품 정보]
📝 [{product_name}] ← 반드시 이 형식을 포함해줘야 해
이상 감지 리뷰 분석 - 상품 ID {product_id}
- 리뷰 수: {len(product_reviews)}
- 평균 평점: {avg_rating:.2f}
- 감성 분석 요약: {sentiment_summary}
- 주요 감성/토픽: {top_categories}

⚠️ 리뷰 평점이 높더라도, 감성 분석 결과가 '부정'이면 반드시 문제로 간주해야 합니다.
GPT는 감성 분석 결과를 신뢰하여 리뷰 텍스트와 함께 분석하세요.

[이상 감지 리뷰 목록]
{sample_reviews}

위 데이터를 기반으로 다음 항목을 포함한 **제품 개선 가이드라인**을 작성해줘:
1. 리뷰와 평점, 감성 분석 등을 통한 문제 원인 분석
2. 자주 언급된 문제 토픽 기반 요약
3. 고객 이탈 방지를 위한 종합적인 개선 전략 제시

각 항목은 반드시 다음 형식을 따라줘:
1. 문제 원인 분석: (소제목 형태로 시작하고, 단락으로 서술)
2. 문제 토픽 요약: (주요 키워드를 강조하며 문장화)
3. 개선 전략 제시: (a, b, c 항목으로 구체적으로 제안)

각 섹션은 마크다운 스타일 또는 HTML 없이 평범한 텍스트로, 깔끔하게 단락 위주로 작성해줘.
"""

    # ✅ GPT 호출
    client = AsyncOpenAI(api_key="OPENAI_API_KEY")
    try:
        response = await client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "너는 제품 리뷰 분석 전문가야."},
                {"role": "user", "content": prompt}
            ]
        )
        result = response.choices[0].message.content
        await redis.set(cache_key, result, ex=86400)
        return {"guideline": result}
    except Exception:
        raise HTTPException(status_code=500, detail="GPT 호출 실패")
# ########
@app.get("/api/anomaly-guideline-today/{product_id}")
async def generate_guideline_today(product_id: int):
    redis = await get_redis()
    cache_key = f"guideline:product:{product_id}:today"

    cached = await redis.get(cache_key)
    if cached:
        return {"guideline": cached}

    try:
        summary = await anomaly_summary()
        today_reviews = summary.get("today", {}).get("anomaly_reviews", [])
        product_reviews = [
            r for r in today_reviews
            if str(r.get("product_id")) == str(product_id)
        ]
    except Exception:
        raise HTTPException(status_code=500, detail="anomaly-summary 호출 실패")

    if not product_reviews:
        return {"guideline": f"ID {product_id}에 대한 오늘 리뷰가 없어 가이드라인을 생성할 수 없습니다."}

    def get_sentiment_label(code):
        return {0: "부정", 1: "중립", 2: "긍정"}.get(code, "알 수 없음")

    sentiment_counter = Counter(r['sentiment'] for r in product_reviews)
    sentiment_summary = f"부정: {sentiment_counter.get(0, 0)}개, 중립: {sentiment_counter.get(1, 0)}개, 긍정: {sentiment_counter.get(2, 0)}개"

    avg_rating = sum(r["rating"] for r in product_reviews if r.get("rating")) / len(product_reviews)

    category_counter = Counter(
        c.strip() for r in product_reviews for c in r.get("category", "").split(",") if c.strip()
    )
    top_categories = ", ".join(f"{cat}({cnt}회)" for cat, cnt in category_counter.most_common(5))

    sample_reviews = "\n".join([
        f'- "{r["sentence"]}" (평점: {r["rating"]}, 감성: {get_sentiment_label(r["sentiment"])})'
        for r in product_reviews
    ])
    product_name = product_reviews[0].get("product_name", "제품명 없음")  # 첫 리뷰에서 추출
    prompt = f"""
    [상품 정보]
📝 [{product_name}] ← 반드시 이 형식을 포함해줘야 해
[오늘 리뷰 기반 분석] - 상품 ID {product_id}
- 리뷰 수: {len(product_reviews)}
- 평균 평점: {avg_rating:.2f}
- 감성 분석 요약: {sentiment_summary}
- 주요 감성/토픽: {top_categories}

⚠️ 리뷰 평점이 높더라도, 감성 분석 결과가 '부정'이면 반드시 문제로 간주해야 합니다.
GPT는 감성 분석 결과를 신뢰하여 리뷰 텍스트와 함께 분석하세요.

[이상 감지 리뷰 목록]
{sample_reviews}

위 데이터를 기반으로 다음 항목을 포함한 **제품 개선 가이드라인**을 작성해줘:
1. 리뷰와 평점, 감성 분석 등을 통한 문제 원인 분석
2. 자주 언급된 문제 토픽 기반 요약
3. 고객 이탈 방지를 위한 종합적인 개선 전략 제시

각 항목은 반드시 다음 형식을 따라줘:
1. 문제 원인 분석: (소제목 형태로 시작하고, 단락으로 서술)
2. 문제 토픽 요약: (주요 키워드를 강조하며 문장화)
3. 개선 전략 제시: (a, b, c 항목으로 구체적으로 제안)

각 섹션은 마크다운 스타일 또는 HTML 없이 평범한 텍스트로, 깔끔하게 단락 위주로 작성해줘.
"""

    client = AsyncOpenAI(api_key="OPENAI_API_KEY")
    try:
        response = await client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "너는 제품 리뷰 분석 전문가야."},
                {"role": "user", "content": prompt}
            ]
        )
        result = response.choices[0].message.content
        await redis.set(cache_key, result, ex=3600)  # 1시간 캐시
        return {"guideline": result}
    except Exception:
        raise HTTPException(status_code=500, detail="GPT 호출 실패")

import os  # ✅ 이 줄 추가

@app.get("/api/anomaly-guideline-monthly/{year}/{month}")
async def generate_monthly_guideline(year: int, month: int):
    redis = await get_redis()
    cache_key = f"guideline:monthly:{year}-{month:02d}"

    cached = await redis.get(cache_key)
    if cached:
        return {"guideline": cached}

    try:
        # 🔹 기존 3개 API 호출
        trend_task = get_anomaly_trend(year, month)
        category_task = get_anomaly_shoes_category(year, month)
        product_task = get_anomaly_products(year, month)

        trend, category, products = await asyncio.gather(trend_task, category_task, product_task)

        # 🔸 데이터 정리
        trend_text = "\n".join([
            f"- {row['date']}: 부정 리뷰 {row['negative_count']}건, 저평점 {row['low_rating_count']}건"
            for row in trend
        ])

        category_data = category.get("data", [])
        category_text = "\n".join([
            f"- {row['category']}: {row['anomaly_count']}건"
            for row in category_data
        ])

        product_text = "\n".join([
            f"- {row['product_name']} (ID: {row['product_id']}): {row['anomaly_count']}건"
            for row in products
        ])

        # 🔸 GPT 프롬프트 구성
        prompt = f"""
[2025년 {month}월] 이상 감지 리뷰 통계 분석

1. 📈 이상 감지 추이
{trend_text}

2. 📦 카테고리별 이상 감지 분포
{category_text}

3. 🔍 이상 감지 제품 Top 10
{product_text}

이 데이터를 바탕으로 아래 항목을 포함한 월간 종합 개선 가이드라인을 작성해주세요:

- 전반적인 이상 감지 트렌드 요약
- 주의가 필요한 제품군 분석
- 주요 제품에 대한 문제 요약 및 개선 방향
- 리뷰 감성 및 평점 분포의 차이에 대한 시사점

친절하고 명확하게, 자연어 문장으로 서술해주세요.
"""

        # 🔹 GPT 호출
        client = AsyncOpenAI(api_key="OPENAI_API_KEY")
        response = await client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "너는 리뷰 분석 전문가야. 데이터를 분석하고 인사이트를 제공해줘."},
                {"role": "user", "content": prompt}
            ]
        )

        guideline = response.choices[0].message.content
        await redis.set(cache_key, guideline, ex=86400)

        return {"guideline": guideline}

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# @app.delete("/api/anomaly-guideline/{product_id}/cache")
# async def delete_guideline_cache(product_id: int):
#     redis = await get_redis()
#     cache_key = f"guideline:product:{product_id}"
#     deleted = await redis.delete(cache_key)
#     return {
#         "cache_key": cache_key,
#         "deleted": bool(deleted),
#         "message": "캐시 삭제 완료" if deleted else "캐시에 해당 키가 존재하지 않았습니다."
#     }





@app.get("/")
async def home(request: Request):
    """ 기본 경로에서 index.html을 렌더링하고, 이상 감지 리뷰 데이터를 전달 """
    redis = await get_redis()
    reviews = await redis.lrange("negative_reviews", 0, -1)

    try:
        negative_reviews = [json.loads(review.decode()) for review in reviews] if reviews else []
    except json.JSONDecodeError as e:
        print(f"❌ JSON 디코딩 오류: {e}")
        negative_reviews = []

    return templates.TemplateResponse("index.html", {
        "request": request,
        "today_date": datetime.today().strftime("%Y-%m-%d"),
        "negative_reviews": negative_reviews
    })

# # 🔹 이상 감지 페이지 렌더링
# @app.get("/anomaly-detection.html")
# async def anomaly_detection_page(request: Request):
#     redis = await get_redis()
#     reviews = await redis.lrange("negative_reviews", 0, -1)

#     # ✅ 데이터 디코딩 과정 개선 (json.loads() 사용)
#     try:
#         negative_reviews = [json.loads(review.decode()) for review in reviews] if reviews else []
#     except json.JSONDecodeError as e:
#         print(f"❌ JSON 디코딩 오류: {e}")
#         negative_reviews = []

#     return templates.TemplateResponse("anomaly-detection.html", {
#         "request": request,
#         "today_date": datetime.today().strftime("%Y-%m-%d"),
#         "negative_reviews": negative_reviews
#     })

# ✅ 백그라운드 작업 실행
@app.on_event("startup")
async def startup_event():
    try:
        """ 서버 시작 시 자동으로 테스트 알림을 발행 (1회 실행) """
        print("🔹 서버 시작됨. Redis 테스트 메시지 발행!")
        asyncio.ensure_future(redis_listener())  # 🔥 Redis Pub/Sub 리스너 실행
        asyncio.ensure_future(check_table_updates_task())  # 🔥 MySQL 변경 감지 태스크 실행
        # ✅ Redis에 저장된 부정 리뷰 확인 (이전 리뷰 로드)
        asyncio.ensure_future(check_redis_negative_reviews())
    except Exception as e:
        print(f"🚨 Redis 및 MySQL 변경 감지 프로세스 시작 실패: {e}")
#--------------------------------------------------------#