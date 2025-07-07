# Base image with Python 3.10 and pip
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install essential system libraries
RUN apt-get update && \
    apt-get install -y gcc curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY validation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy validation script and helper functions
COPY validation/validate_task.py .
COPY helper_functions/ ./helper_functions/

# Default command (can be overridden by ECS or docker run)
CMD ["python3", "validate_task.py"]