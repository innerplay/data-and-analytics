# 1️⃣  use a slim, current Python base
FROM python:3.11-slim

# 2️⃣  create working directory
WORKDIR /app

# 3️⃣  copy dependency descriptors first for cached layer
COPY requirements.txt .

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc \
 && pip install --no-cache-dir -r requirements.txt \
 && apt-get purge -y --auto-remove gcc

# 4️⃣  copy source code
COPY . .

# 5️⃣  set environment variables (override at runtime)
ENV PYTHONUNBUFFERED=1 \
	GOOGLE_APPLICATION_CREDENTIALS=/app/data-extractor-gcp-credentials.json

# 6️⃣  default entry point
CMD ["python", "main.py"]