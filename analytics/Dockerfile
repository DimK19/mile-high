FROM apache/spark:3.5.1

USER root
WORKDIR /app

# Install pip (Apache Spark image is minimal)
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Create writable dirs for Ivy + checkpoints
RUN mkdir -p /tmp/.ivy2 /tmp/spark-checkpoints/traffic_5m && \
    chown -R spark:spark /tmp/.ivy2 /tmp/spark-checkpoints

USER spark
COPY spark_job.py .
