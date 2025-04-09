import openai

client = os.getenv("OPENAI_API_KEY")  # ← 발급받은 키 사용

prompt = "이 제품 리뷰에서 고객이 느낀 불만과 개선할 점을 알려줘:\n\n'신발이 너무 작고 발이 아팠어요. 다시는 안 살래요.'"

response = client.chat.completions.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "system", "content": "너는 고객 피드백 분석 전문가야."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.7,
    max_tokens=500
)

print(response.choices[0].message.content)

