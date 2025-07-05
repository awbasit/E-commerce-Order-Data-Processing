# Base Spark image
FROM apache/spark-py:3.4.1

# Set working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY validate_task.py /app/
COPY ../helper_functions /app/helper_functions

# Entry point
ENTRYPOINT ["python3", "validate_task.py"]