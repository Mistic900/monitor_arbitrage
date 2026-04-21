FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements_server.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements_server.txt

# Copy application
COPY arbitrage_monitor_v2.py .

# Create directory for database and logs
RUN mkdir -p /app/data /app/logs

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import httpx; httpx.get('http://localhost:8080/health', timeout=5.0)" || exit 1

# Expose health check port
EXPOSE 8080

# Run as non-root user
RUN useradd -m -u 1000 arbbot && chown -R arbbot:arbbot /app
USER arbbot

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV LOG_FILE=/app/logs/arbitrage_monitor.log
ENV DB_PATH=/app/data/arbitrage_opportunities.db

# Run
CMD ["python", "-u", "arbitrage_monitor_v2.py"]
