FROM python:3.13.3-slim

LABEL maintainer="Jurij <js18.user@gmail.com>" 

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

ENV PIP_ROOT_USER_ACTION=ignore

RUN pip install -r requirements.txt

COPY data.html .

COPY create_tables.sql .

COPY cv.pdf

COPY urls.py .

COPY jit.py .

COPY mod.py .

RUN useradd -u 8888 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 80

CMD ["python", "mod.py"]
