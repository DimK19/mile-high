FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vision_worker.py .
COPY .env .

EXPOSE 8000
CMD ["python", "-u", "vision_worker.py"]
