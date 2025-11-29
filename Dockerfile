FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    bluetooth \
    bluez \
    libbluetooth-dev \
    pkg-config \
    libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    bleak \
    prometheus-client \
    asyncio-mqtt

WORKDIR /app

COPY ble_collector.py /app/

RUN chmod +x /app/ble_collector.py

EXPOSE 8000

CMD ["python", "/app/ble_collector.py"]
