# AWS Lambda 공식 Python 3.9 베이스 이미지 사용
FROM public.ecr.aws/lambda/python:3.12

# 의존성 설치
RUN pip3 install beautifulsoup4 requests pyarrow boto3 smart_open psycopg2-binary python-dotenv

# Lambda 핸들러 파일 복사
COPY clien_lambda.py ${LAMBDA_TASK_ROOT}
COPY common_utils.py ${LAMBDA_TASK_ROOT}
COPY settings.py ${LAMBDA_TASK_ROOT}
COPY .env.lambda ${LAMBDA_TASK_ROOT}

# Lambda 실행을 위한 엔트리포인트 설정
CMD ["clien_lambda.detail"]