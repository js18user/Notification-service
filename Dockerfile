# syntax=docker/dockerfile:1

FROM python:3.12-slim

LABEL maintainer="Jurij <js18.user@gmail.com>"

ENV PYTHONDONTWRITEBYTECODE=1

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

RUN adduser --disabled-password --gecos '' appuser

USER appuser

COPY data.html .

COPY create_tables.sql .

COPY urls.py .

COPY asyncpg_pool.py .

COPY mod.py .

EXPOSE 80

CMD ["mod.py" ]

ENTRYPOINT ["python"]












