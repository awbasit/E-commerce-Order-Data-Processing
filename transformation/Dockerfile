# Use Bitnami Spark base image
FROM bitnami/spark:3.4.1

# Set working directory
WORKDIR /app

# Switch to root to install OS-level packages
USER root

# Install Python + dependencies
COPY transformation/requirements.txt .

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --no-cache-dir -r requirements.txt && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy transformation code and helper functions
COPY transformation/transform_task.py /app/
COPY helper_functions /app/helper_functions

# Set default user back to non-root (Bitnami uses `1001`)
USER 1001

# Set entry point
ENTRYPOINT ["python3", "transform_task.py"]
