FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vision_worker.py .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s \
  CMD curl -f http://localhost:8000/metrics || exit 1

CMD ["python", "-u", "vision_worker.py"]