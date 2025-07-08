# Multi-stage build for better reliability
FROM openjdk:11-jre-slim as base

# Install curl and basic tools
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Download Spark and AWS JARs in separate stage
FROM base as jars-downloader

WORKDIR /tmp/jars

# Download JARs with comprehensive retry logic
RUN echo "Downloading Hadoop AWS JAR..." && \
    for i in {1..10}; do \
        echo "Attempt $i for hadoop-aws jar..." && \
        curl -L --retry 10 --retry-delay 30 --retry-max-time 0 \
             --connect-timeout 120 --max-time 1200 \
             -f -o hadoop-aws-3.3.1.jar \
             "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar" && \
        echo "Successfully downloaded hadoop-aws jar" && break || \
        (echo "Attempt $i failed, waiting before retry..." && sleep $((i * 10))); \
    done && \
    test -f hadoop-aws-3.3.1.jar || (echo "Failed to download hadoop-aws jar after all attempts" && exit 1)

RUN echo "Downloading AWS SDK JAR..." && \
    for i in {1..10}; do \
        echo "Attempt $i for aws-sdk jar..." && \
        curl -L --retry 10 --retry-delay 30 --retry-max-time 0 \
             --connect-timeout 120 --max-time 1200 \
             -f -o aws-java-sdk-bundle-1.11.901.jar \
             "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar" && \
        echo "Successfully downloaded aws-sdk jar" && break || \
        (echo "Attempt $i failed, waiting before retry..." && sleep $((i * 10))); \
    done && \
    test -f aws-java-sdk-bundle-1.11.901.jar || (echo "Failed to download aws-sdk jar after all attempts" && exit 1)

# Verify downloads
RUN ls -la *.jar && \
    echo "JAR file sizes:" && \
    du -h *.jar

# Final stage with Spark
FROM bitnami/spark:3.4.1

# Switch to root for installations
USER root

WORKDIR /app

# Copy requirements first for better caching
COPY transformation/requirements.txt .

# Install Python dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy JARs from previous stage
COPY --from=jars-downloader /tmp/jars/*.jar /opt/bitnami/spark/jars/

# Verify JARs are in place
RUN ls -la /opt/bitnami/spark/jars/hadoop-aws* /opt/bitnami/spark/jars/aws-java-sdk*

# Copy application code
COPY transformation/transform_task.py /app/
COPY helper_functions /app/helper_functions/

# Set proper permissions
RUN chown -R 1001:1001 /app

# Switch back to spark user
USER 1001

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/bitnami/spark
ENV JAVA_HOME=/opt/bitnami/java

# Entry point
ENTRYPOINT ["python3", "transform_task.py"]