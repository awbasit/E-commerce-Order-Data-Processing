# Base Spark image
FROM bitnami/spark:3.4.1

# Set working directory
WORKDIR /app

# Switch to root to install system packages
USER root

# Install dependencies
COPY transformation/requirements.txt .
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get clean && \
    apt-get update && \
    apt-get install -y python3-pip && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/*

# Copy application code
COPY transformation/transform_task.py /app/
COPY helper_functions /app/helper_functions

# Entry point
ENTRYPOINT ["python3", "transform_task.py"]