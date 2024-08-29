FROM python:3.8

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# CMD ["python", "kafka_to_es.py"]
# CMD python -m http.server