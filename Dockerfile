FROM python:3.12-slim

# Install ffmpeg and minimal deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app/ ./app/

# Data volume for SQLite
RUN mkdir -p /data
VOLUME /data

EXPOSE 8443

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8443/')" || exit 1

CMD ["python", "-m", "app.bot"]
