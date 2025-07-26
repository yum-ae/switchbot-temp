

# Prometheusテストデータ書き込み用のシンプルなスクリプト
import time
from prometheus_client import Gauge, CollectorRegistry, start_http_server, generate_latest

PROMETHEUS_PORT = 8000
TEST_DEVICE_ADDRESS = "D4:35:34:35:68:4D"

registry = CollectorRegistry()
temperature_gauge = Gauge('ble_temperature_celsius', 'Temperature from BLE beacon in Celsius', ['device_address'], registry=registry)
humidity_gauge = Gauge('ble_humidity_percent', 'Humidity from BLE beacon in percent', ['device_address'], registry=registry)
last_update_gauge = Gauge('ble_last_update_timestamp', 'Last update timestamp from BLE beacon', ['device_address'], registry=registry)

def write_test_metrics():
    now = time.time()
    temp = 23.5
    hum = 55.0
    print(f"[TEST] Writing test metrics: temp={temp}, hum={hum}, ts={now}")
    temperature_gauge.labels(device_address=TEST_DEVICE_ADDRESS).set(temp)
    humidity_gauge.labels(device_address=TEST_DEVICE_ADDRESS).set(hum)
    last_update_gauge.labels(device_address=TEST_DEVICE_ADDRESS).set(now)

if __name__ == "__main__":
    start_http_server(PROMETHEUS_PORT, registry=registry)
    print(f"Prometheus metrics server started on port {PROMETHEUS_PORT}")
    print(f"Metrics available at: http://localhost:{PROMETHEUS_PORT}/metrics")
    while True:
        write_test_metrics()
        metrics_output = generate_latest(registry).decode('utf-8')
        print("--- Prometheus Metrics Output ---")
        print(metrics_output)
        print("-------------------------------")
        time.sleep(10)