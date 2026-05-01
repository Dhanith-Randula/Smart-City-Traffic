FROM python:3.11-slim
WORKDIR /app
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/traffic_producer.py .
CMD ["python", "traffic_producer.py", "--interval", "1"]
