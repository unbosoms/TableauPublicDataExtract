FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY get_data.py s3_store.py lambda_handler.py ./

CMD ["lambda_handler.handler"]
