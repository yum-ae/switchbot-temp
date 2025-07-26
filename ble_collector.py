# BLE温湿度データをPrometheusにエクスポートするスクリプト
import time
import asyncio
import binascii
from prometheus_client import Gauge, CollectorRegistry, start_http_server, generate_latest
from bleak import BleakScanner

# 定数
PROMETHEUS_PORT = 8000
TARGET_MAC_ADDRESS = "D4:35:34:35:68:4D"
MANUFACTURER_ID = 0x2409

# Prometheusメトリクスのセットアップ
registry = CollectorRegistry()
temperature_gauge = Gauge(
    'ble_temperature_celsius', 'Temperature from BLE beacon in Celsius', ['device_address'], registry=registry)
humidity_gauge = Gauge(
    'ble_humidity_percent', 'Humidity from BLE beacon in percent', ['device_address'], registry=registry)
last_update_gauge = Gauge(
    'ble_last_update_timestamp', 'Last update timestamp from BLE beacon', ['device_address'], registry=registry)


def write_metrics(device_address: str, temperature: float, humidity: float):
    """Prometheusメトリクスを書き込む"""
    now = time.time()
    print(f"[METRICS] Writing: temp={temperature}, hum={humidity}, ts={now}")
    temperature_gauge.labels(device_address=device_address).set(temperature)
    humidity_gauge.labels(device_address=device_address).set(humidity)
    last_update_gauge.labels(device_address=device_address).set(now)


def parse_temperature_humidity(data: bytes) -> None:
    """メーカー固有データから温度・湿度をパースしメトリクスに反映"""
    if len(data) < 11:
        print("Invalid manufacturer data length")
        return
    sign = data[9] & 0b10000000
    temperature_decimals = data[8] & 0b00001111
    temperature = (data[9] & 0b01111111)
    if sign == 0:
        temperature = -temperature
    humidity = data[10] & 0b01111111
    print(f"Temperature: {temperature}.{temperature_decimals}°C, Humidity: {humidity}%")
    temperature_float = float(f"{temperature}.{temperature_decimals}")
    write_metrics(TARGET_MAC_ADDRESS, temperature_float, humidity)


def ble_callback(device, advertisement_data):
    if device.address.upper() == TARGET_MAC_ADDRESS:
        manufacturer_data = advertisement_data.manufacturer_data.get(MANUFACTURER_ID)
        if manufacturer_data:
            # print(f"Raw Manufacturer Data: {binascii.hexlify(manufacturer_data).decode()}")
            parse_temperature_humidity(manufacturer_data)


async def scan_ble():
    """BLEスキャンを実行し、コールバックでデータ処理"""
    print(f"Scanning for BLE device with MAC address: {TARGET_MAC_ADDRESS}...")
    scanner = BleakScanner(ble_callback)
    await scanner.start()
    await asyncio.sleep(5)  # 5秒スキャン
    await scanner.stop()
    print("Scan complete.")


async def main():
    start_http_server(PROMETHEUS_PORT, registry=registry)
    print(f"Prometheus metrics server started on port {PROMETHEUS_PORT}")
    print(f"Metrics available at: http://localhost:{PROMETHEUS_PORT}/metrics")
    while True:
        metrics_output = generate_latest(registry).decode('utf-8')
        print("--- Prometheus Metrics Output ---")
        print(metrics_output)
        print("-------------------------------")
        time.sleep(10)
        await scan_ble()
        await asyncio.sleep(55)


asyncio.run(main())


