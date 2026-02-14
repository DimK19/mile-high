# Use official Azure Functions Python base image
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Copy your function app code
WORKDIR /home/site/wwwroot
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port (optional, runtime handles it)
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    FUNCTIONS_WORKER_RUNTIME=python

# Command is inherited from the base image

