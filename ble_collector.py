

# BLE温湿度データをPrometheusにエクスポートするスクリプト（複数MAC対応）
import time
import asyncio
from prometheus_client import Gauge, CollectorRegistry, start_http_server, generate_latest
from bleak import BleakScanner

PROMETHEUS_PORT = 8000
MANUFACTURER_ID = 0x2409

# 監視対象MACアドレス（大文字で統一）
TARGET_MAC_ADDRESSES = [
    "D4:35:34:35:68:4D",
    "F2:B2:02:06:7C:49",
]

registry = CollectorRegistry()
temperature_gauge = Gauge('ble_temperature_celsius', 'Temperature from BLE beacon in Celsius', ['device_address'], registry=registry)
humidity_gauge = Gauge('ble_humidity_percent', 'Humidity from BLE beacon in percent', ['device_address'], registry=registry)
last_update_gauge = Gauge('ble_last_update_timestamp', 'Last update timestamp from BLE beacon', ['device_address'], registry=registry)


def write_metrics(device_address, temperature_float, humidity):
    now = time.time()
    print(f"[DEBUG] Writing metrics for {device_address}: temp={temperature_float}, hum={humidity}, ts={now}")
    temperature_gauge.labels(device_address=device_address).set(temperature_float)
    humidity_gauge.labels(device_address=device_address).set(humidity)
    last_update_gauge.labels(device_address=device_address).set(now)


def parse_beacon_data(data: bytes):
    """
    新しい仕様のBLEビーコンデータから温度・湿度をパース
    仕様: temp = ((data[10] & 0x0F) * 0.1 + (data[11] & 0x7F)) * (((data[11] & 0x80) > 0) ? 1 : -1);
    humidity = data[12] & 0x7F
    """

    # if len(data) < 12:
    #     print(f"Manufacturer data too short: {len(data)} bytes")
    #     return None
    try:
        temp_byte_10 = data[10]
        temp_byte_11 = data[11]
        # humidity_byte_12 = data[12]
    except IndexError:
        print("Insufficient data bytes for temperature/humidity calculation.")
        return None
    temp_decimal_part = (temp_byte_10 & 0x0F) * 0.1
    temp_integer_part = temp_byte_11 & 0x7F
    temp_sign = 1 if (temp_byte_11 & 0x80) > 0 else -1
    temperature = (temp_decimal_part + temp_integer_part) * temp_sign
    # humidity = humidity_byte_12 & 0x7F
    return {
        "temperature_celsius": round(temperature, 2),
        # "humidity_percent": humidity
    }


def parse_legacy_data(data: bytes):
    """
    旧仕様のデータパース（既存ロジック）
    """
    if len(data) < 11:
        print("Invalid manufacturer data length")
        return None
    sign = data[9] & 0b10000000
    temperature_decimals = data[8] & 0b00001111
    temperature = (data[9] & 0b01111111)
    if sign == 0:
        temperature = -temperature
    humidity = data[10] & 0b01111111
    temperature_float = float(f"{temperature}.{temperature_decimals}")
    return {
        "temperature_celsius": temperature_float,
        "humidity_percent": humidity
    }


def handle_advertisement(device, advertisement_data):
    mac = device.address.upper()
    print(f"[DEBUG] Advertisement received from {mac}")
    if mac not in TARGET_MAC_ADDRESSES:
        print(f"[DEBUG] {mac} is not a target address. Skipping.")
        return
    manufacturer_data = advertisement_data.manufacturer_data[2409]
    # manufacturer_data = advertisement_data.manufacturer_data.get(MANUFACTURER_ID)
    if not manufacturer_data:
        print(f"[DEBUG] No manufacturer data for {mac}. Skipping.")
        return
    print(f"[DEBUG] Manufacturer data for {mac}: {manufacturer_data.hex() if hasattr(manufacturer_data, 'hex') else manufacturer_data}")
    # MACアドレスごとにパース関数を切り替え
    if mac == "D4:35:34:35:68:4D":
        print(f"[DEBUG] Using legacy parser for {mac}")
        parsed = parse_legacy_data(manufacturer_data)
    elif mac == "F2:B2:02:06:7C:49":
        print(f"[DEBUG] Using beacon parser for {mac}")
        parsed = parse_beacon_data(manufacturer_data)
    else:
        print(f"[DEBUG] No parser defined for {mac}")
        return
    if not parsed:
        print(f"[DEBUG] Could not parse data for {mac}. Data: {manufacturer_data.hex() if hasattr(manufacturer_data, 'hex') else manufacturer_data}")
        return
    print(f"[DEBUG] Parsed result for {mac}: {parsed}")
    print(f"[{mac}] Temperature: {parsed['temperature_celsius']}°C, Humidity: {parsed['humidity_percent']}%")
    write_metrics(mac, parsed['temperature_celsius'], parsed['humidity_percent'])


async def scan_ble():
    print(f"[DEBUG] Scanning for BLE devices: {', '.join(TARGET_MAC_ADDRESSES)}...")
    scanner = BleakScanner(handle_advertisement)
    await scanner.start()
    await asyncio.sleep(5)
    await scanner.stop()
    print("[DEBUG] Scan complete.")


async def main():
    start_http_server(PROMETHEUS_PORT, registry=registry)
    print(f"[DEBUG] Prometheus metrics server started on port {PROMETHEUS_PORT}")
    print(f"[DEBUG] Metrics available at: http://localhost:{PROMETHEUS_PORT}/metrics")
    while True:
        metrics_output = generate_latest(registry).decode('utf-8')
        print("--- Prometheus Metrics Output ---")
        print(metrics_output)
        print("-------------------------------")
        await scan_ble()
        await asyncio.sleep(55)


if __name__ == "__main__":
    asyncio.run(main())
