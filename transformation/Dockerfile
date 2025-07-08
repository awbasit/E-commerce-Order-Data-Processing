# Base image with Java, Spark, Hadoop and Python preconfigured
FROM apache/spark-py:3.4.1

# Set working directory
WORKDIR /app

# Install Python packages
COPY transformation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code
COPY transformation/transform_task.py /app/
COPY helper_functions /app/helper_functions

# Set Spark environment variables
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    HADOOP_HOME=/opt/hadoop \
    SPARK_LOCAL_DIRS=/tmp/spark

# Entry point
ENTRYPOINT ["python3", "transform_task.py"]