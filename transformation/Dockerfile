# Base Spark image
FROM bitnami/spark:3.4.1

# Set working directory
WORKDIR /app

# Switch to root to install system packages
USER root

# Install dependencies
COPY transformation/requirements.txt .
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y python3-pip && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/*

# Install curl first (important!)
RUN apt-get update && apt-get install -y curl   
 
# Install Hadoop AWS and AWS SDK dependencies
RUN apt-get update && apt-get install -y curl && \
    mkdir -p /opt/bitnami/spark/jars && \
    curl -o /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
    
# Copy application code
COPY transformation/transform_task.py /app/
COPY helper_functions /app/helper_functions

# Entry point
ENTRYPOINT ["python3", "transform_task.py"]