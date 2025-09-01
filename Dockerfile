FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8000

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libjpeg62-turbo-dev \
    zlib1g-dev \
    libopenjp2-7 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY app.py ./
# COPY templates ./templates   # if you use render_template()

EXPOSE 8000
CMD ["python","-m","gunicorn","-b","0.0.0.0:8000","app:app","--workers","2","--threads","4","--timeout","120","--forwarded-allow-ips=*"]
