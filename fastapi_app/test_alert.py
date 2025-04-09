import asyncio
from main import send_negative_review_alert

async def test_alert():
    await send_negative_review_alert({
        "review_id": "72187464",
        "product_id": "595039",
        "product_name": "테스트 상품",
        "brand": "테스트 브랜드",
        "price": "10000",
        "image_url": "https://example.com/image.jpg",
        "category": "기타",
        "sentence": "주문하고 다음날 왔네요",
        "rating": 5,
        "sentiment": 0,
        "review_date": "2025-03-17"
    })

    await send_negative_review_alert({
        "review_id": "72188808",
        "product_id": "1367805",
        "product_name": "테스트 상품2",
        "brand": "테스트 브랜드2",
        "price": "20000",
        "image_url": "https://example.com/image2.jpg",
        "category": "디자인,착용감",
        "sentence": "뒤꿈치에 푹신한걸 깔아서 그런지 오히려 불편했습니다",
        "rating": 3,
        "sentiment": 0,
        "review_date": "2025-03-17"
    })

# 이벤트 루프 실행
asyncio.run(test_alert())