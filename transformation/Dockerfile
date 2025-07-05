# Base Spark image
FROM apache/spark-py:3.4.1

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY transform_task.py /app/
COPY ../helper_functions /app/helper_functions

# Entry point
ENTRYPOINT ["python3", "transform_task.py"]