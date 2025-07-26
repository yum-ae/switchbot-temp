import asyncio
import binascii
import datetime
from typing import Optional
from bleak import BleakScanner
from prometheus_client import Gauge, CollectorRegistry, start_http_server, generate_latest
import time

# 設定
TARGET_MAC_ADDRESS = "D4:35:34:35:68:4D"  # あなたのビーコンのMACアドレス
MANUFACTURER_ID = 0x2409
PROMETHEUS_PORT = 8000

# Prometheusメトリクス定義
registry = CollectorRegistry()
temperature_gauge = Gauge('ble_temperature_celsius', 
                         'Temperature from BLE beacon in Celsius', 
                         ['device_address'], 
                         registry=registry)
humidity_gauge = Gauge('ble_humidity_percent', 
                      'Humidity from BLE beacon in percent', 
                      ['device_address'], 
                      registry=registry)
last_update_gauge = Gauge('ble_last_update_timestamp', 
                         'Last update timestamp from BLE beacon', 
                         ['device_address'], 
                         registry=registry)

def parse_temperature_humidity(data: bytes, device_address: str) -> Optional[tuple]:
    """BLEビーコンデータから温度と湿度を解析"""
    print(f"parse_temperature_humidity called for {device_address}, data: {binascii.hexlify(data)}")
    if len(data) < 11:
        print("Invalid manufacturer data length")
        return None
        
    sign = data[9] & 0b10000000
    temperature_decimals = data[8] & 0b00001111
    temperature = (data[9] & 0b01111111)
    
    if sign == 0:
        temperature = -temperature
        
    humidity = data[10] & 0b01111111
    
    print(f"Temperature: {temperature}.{temperature_decimals}°C, Humidity: {humidity}%")
    
    temperature_str = f"{temperature}.{temperature_decimals}"
    temperature_float = float(temperature_str)
    
    # Prometheusメトリクスを更新
    temperature_gauge.labels(device_address=device_address).set(temperature_float)
    humidity_gauge.labels(device_address=device_address).set(humidity)
    last_update_gauge.labels(device_address=device_address).set(time.time())
    
    print("Data updated in Prometheus metrics")
    return temperature_float, humidity

async def scan_ble():
    """BLEデバイスをスキャンしてデータを取得"""
    def callback(device, advertisement_data):
        print(f"Found device: {device.address}, RSSI: {device.rssi}")
        if advertisement_data.manufacturer_data:
            print(f"  manufacturer_data: {advertisement_data.manufacturer_data}")
        if device.address.upper() == TARGET_MAC_ADDRESS:
            print(f"  Target device matched: {device.address}")
            if MANUFACTURER_ID in advertisement_data.manufacturer_data:
                manufacturer_data = advertisement_data.manufacturer_data[MANUFACTURER_ID]
                if manufacturer_data:
                    print(f"  Manufacturer data for target: {binascii.hexlify(manufacturer_data)}")
                    parse_temperature_humidity(manufacturer_data, device.address)
            else:
                print(f"  Manufacturer ID {hex(MANUFACTURER_ID)} not found in manufacturer_data")
    print(f"Scanning for BLE device with MAC address: {TARGET_MAC_ADDRESS}...")
    scanner = BleakScanner(callback)
    await scanner.start()
    await asyncio.sleep(5)  # 5秒スキャン
    await scanner.stop()
    print("Scan complete.")

async def main():
    """メイン処理"""
    # Prometheusメトリクスサーバーを開始
    start_http_server(PROMETHEUS_PORT, registry=registry)
    print(f"Prometheus metrics server started on port {PROMETHEUS_PORT}")
    print(f"Metrics available at: http://localhost:{PROMETHEUS_PORT}/metrics")
    
    # BLEスキャンループ
    while True:
        try:
            await scan_ble()
            # メトリクスエンドポイントの内容をprint（デバッグ用）
            metrics_output = generate_latest(registry).decode('utf-8')
            print("--- Prometheus Metrics Output ---")
            print(metrics_output)
            print("-------------------------------")
            await asyncio.sleep(55)  # 55秒待機
        except KeyboardInterrupt:
            print("Stopping BLE scanner...")
            break
        except Exception as e:
            print(f"Error during BLE scanning: {e}")
            await asyncio.sleep(10)  # エラー時は10秒待機

if __name__ == "__main__":
    asyncio.run(main())