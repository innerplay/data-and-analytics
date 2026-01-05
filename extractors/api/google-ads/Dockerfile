FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc \
 && pip install --no-cache-dir -r requirements.txt \
 && apt-get purge -y --auto-remove gcc

COPY . .

ENV PYTHONUNBUFFERED=1 \
	GOOGLE_ADS_CONFIG=/app/google_ads.yaml

CMD ["python", "main.py"]