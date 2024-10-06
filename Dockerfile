# syntax=docker/dockerfile:1

FROM python:3.12.4-alpine3.20

LABEL maintainer="Jurij <js18.user@gmail.com>"

ENV PYTHONDONTWRITEBYTECODE=1

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

COPY data.html .

COPY create_tables.sql .

COPY urls.py .

COPY prometheus.yml .

COPY mod.py .

EXPOSE 80

CMD ["mod.py" ]

ENTRYPOINT ["python"]
