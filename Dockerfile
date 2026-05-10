# syntax=docker/dockerfile:1

FROM python:3.12-slim

LABEL maintainer="Jurij <js18.user@gmail.com>"

ENV PYTHON_GIL=0 

ENV PYTHONDONTWRITEBYTECODE=1

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install -r requirements.txt --root-user-action=ignore

COPY data.html .

COPY gct.html .

COPY create_tables.sql .

COPY urls.py .

COPY asyncpg_pool.py .

COPY jit.py .

COPY modp.py .

EXPOSE 80

CMD ["python", "modp.py","-X", "freethreaded", "-m" "--loop", "asyncio"]
