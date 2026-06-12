FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/  ./schemas/
COPY producer/ ./producer/

CMD ["python", "producer/transaction_generator.py"]
