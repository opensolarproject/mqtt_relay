FROM python:3.12-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mqtt_relay.py .

CMD ["python", "mqtt_relay.py"]
