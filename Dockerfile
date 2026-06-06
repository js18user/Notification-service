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

RUN printf "msp.mcp-service.eu {\n\
    reverse_proxy localhost:8000\n\
}\n\n\
grafana.mcp-service.eu {\n\
    reverse_proxy localhost:8000\n\
}\n\n\
cv.mcp-service.eu {\n\
    root * /app\n\
    rewrite * /cv.pdf\n\
    file_server\n\
}\n\n\
resume.mcp-service.eu {\n\
    root * /app\n\
    rewrite * /cv.pdf\n\
    file_server\n\
}\n" > /app/Caddyfile

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Открываем порты для обычного трафика (80) и SSL (443)
EXPOSE 80
EXPOSE 443

CMD ["./entrypoint.sh"]
