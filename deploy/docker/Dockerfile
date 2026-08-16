FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    bluetooth \
    bluez \
    libbluetooth-dev \
    pkg-config \
    libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml /app/
COPY src /app/src
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["ble-prometheus-collector"]
