FROM python:3.13.3-slim

LABEL maintainer="Jurij <js18.user@gmail.com>" 

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore

# Скачиваем готовый бинарник Caddy напрямую — без gpg ключей и сторонних репозиториев
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -sLf "https://caddyserver.com" -o /usr/bin/caddy \
    && chmod +x /usr/bin/caddy \
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

EXPOSE 80
EXPOSE 443

CMD ["./entrypoint.sh"]
