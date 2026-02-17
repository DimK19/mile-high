FROM python:3.11-slim

WORKDIR /app

# 1. Install system dependencies required for OpenCV
# (libgl1 and libglib2.0 are needed for video processing)
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# 2. Install Python dependencies 
# (Make sure requirements.txt includes: ultralytics, opencv-python-headless)
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the AI Model Weights
# This bakes the model file into the image so it works offline and starts instantly.
# (If you switched to the small model, change this to: COPY yolov8s.pt .)
COPY yolov8n.pt .

# 4. Copy the Application Code
COPY vision_worker.py .
COPY .env .

EXPOSE 8000

# 5. Run the worker
# -u forces unbuffered output so you see logs instantly
CMD ["python", "-u", "vision_worker.py"]