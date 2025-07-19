FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

# Set up working directory
WORKDIR /app

# Install Java for Tika
# First, system packages and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre \
    openjdk-11-jre-headless \
    curl \
    build-essential \
    libpq-dev \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    antiword \
    unrtf \
    libxslt1-dev \
    libxml2-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY resume_parser_NEW_FINAL_with_docs/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Verify Java installation for Tika
RUN echo "Verifying Java installation:" && java -version

# Setup Tika directory
RUN mkdir -p /app/temp/tika \
    && chmod 777 /app/temp/tika

# Copy application code
COPY resume_parser_NEW_FINAL_with_docs/ /app/

# Set Tika environment variables
ENV TIKA_PATH=/app/temp/tika \
    TIKA_LOG_PATH=/app/temp/tika/tika.log \
    TIKA_STARTUP_SLEEP=15 \
    TIKA_STARTUP_MAX_RETRY=3 \
    TIKA_JAVA_OPTIONS="-Xmx1024m" \
    TIKA_SERVER_JAR="https://repo1.maven.org/maven2/org/apache/tika/tika-server/2.9.1/tika-server-2.9.1.jar"

# Pre-download Tika server JAR
RUN mkdir -p /root/.tika && \
    curl -o /root/.tika/tika-server.jar $TIKA_SERVER_JAR

# Initialize Tika
RUN python -c "from src.extractors.tika_config import initialize_tika_server; initialize_tika_server()" || echo "Tika initialization will be done during runtime"

# Create output directories
RUN mkdir -p /app/output /app/logs

# Simple command to run parse_resume.py as the main entry point
# No parameters - these will be provided at runtime when running the container
CMD ["python", "parse_resume.py"] 