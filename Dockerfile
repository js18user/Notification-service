FROM python:3.13.3-slim

LABEL maintainer="Jurij <js18.user@gmail.com>" 

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore
RUN apt-get update && apt-get install -y --no-install-recommends \
    debian-keyring debian-archive-keyring apt-transport-https curl ca-certificates \
    && curl -1sLf 'https://cloudsmith.io' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg \
    && curl -1sLf 'https://cloudsmith.io' | tee /etc/apt/sources.list.p/caddy-stable.list \
    && apt-get update && apt-get install -y --no-install-recommends caddy \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
    
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY data.html .

COPY create_tables.sql .

COPY cv.pdf .

COPY urls.py .

COPY jit.py .

COPY mod.py .

COPY Caddyfile .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 80

CMD ["./entrypoint.sh"]
