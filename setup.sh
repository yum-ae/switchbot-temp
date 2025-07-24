#!/bin/bash

# BLE to Prometheus/Grafana セットアップスクリプト

echo "Setting up BLE to Prometheus/Grafana monitoring system..."

# 1. 必要なディレクトリを作成
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/provisioning/dashboards

# 2. uvをインストール（まだインストールされていない場合）
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
fi

# 3. Python仮想環境を作成し、依存関係をインストール
echo "Creating Python virtual environment and installing dependencies..."
uv venv
source .venv/bin/activate
uv pip sync <(uv pip compile pyproject.toml)

# 4. Grafanaのデータソース設定を配置
cat > grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
EOF

# 5. Docker Composeでサービスを起動
echo "Starting Prometheus and Grafana with Docker Compose..."
docker-compose up -d

echo ""
echo "Setup complete!"
echo ""
echo "Services:"
echo "- Prometheus: http://localhost:9090"
echo "- Grafana: http://localhost:3000 (admin/admin123)"
echo ""
echo "To start the BLE collector:"
echo "1. Activate virtual environment: source .venv/bin/activate"
echo "2. Update TARGET_MAC_ADDRESS in ble_collector.py"
echo "3. Run: python ble_collector.py"
echo ""
echo "Grafana dashboard queries:"
echo "- Temperature: ble_temperature_celsius"
echo "- Humidity: ble_humidity_percent"