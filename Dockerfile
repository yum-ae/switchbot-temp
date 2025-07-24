FROM python:3.11-slim

# システムパッケージのインストール
RUN apt-get update && apt-get install -y \
    bluetooth \
    bluez \
    libbluetooth-dev \
    pkg-config \
    libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

# Pythonライブラリのインストール
RUN pip install --no-cache-dir \
    bleak \
    prometheus-client \
    asyncio-mqtt

# アプリケーションディレクトリ作成
WORKDIR /app

# Pythonスクリプトをコピー
COPY ble_collector.py /app/

# 実行権限付与
RUN chmod +x /app/ble_collector.py

# Prometheusメトリクス公開ポート
EXPOSE 8000

# アプリケーション実行
CMD ["python", "/app/ble_collector.py"]
