import time
import asyncio
from prometheus_client import Gauge, CollectorRegistry, start_http_server, generate_latest
from bleak import BleakScanner

PROMETHEUS_PORT = 8000
MANUFACTURER_ID = 2409

TARGET_MAC_ADDRESSES = [
    "D4:35:34:35:68:4D", # Legacy parser
    "F2:B2:02:06:7C:49", # New beacon parser
]

registry = CollectorRegistry()
temperature_gauge = Gauge('ble_temperature_celsius', 'Temperature from BLE beacon in Celsius', ['device_address'], registry=registry)
humidity_gauge = Gauge('ble_humidity_percent', 'Humidity from BLE beacon in percent', ['device_address'], registry=registry)
last_update_gauge = Gauge('ble_last_update_timestamp', 'Last update timestamp from BLE beacon', ['device_address'], registry=registry)


def write_metrics(device_address, temperature_float, humidity):
    now = time.time()
    print(f"[INFO] Writing metrics for {device_address}: temp={temperature_float}°C, hum={humidity}%, ts={now}")
    temperature_gauge.labels(device_address=device_address).set(temperature_float)
    humidity_gauge.labels(device_address=device_address).set(humidity)
    last_update_gauge.labels(device_address=device_address).set(now)


def parse_beacon_data(data: bytes):
    """
    data[9]: temperature byte
    data[10]: humidity byte
    temperature: first 7 bits are integer part, MSB is sign (0: negative, 1: positive)
    humidity: lower 7 bits for percentage (0-100%)
    """
    try:
        # Manufacturer data is expected to start with the MAC address.
        if len(data) < 11: # Need data[9] and data[10]
            print(f"[ERROR] Manufacturer data too short for new beacon parser. Expected at least 11 bytes, got {len(data)}.")
            return None

        temp_byte = data[9]
        humidity_byte = data[10]



        temp_byte_decimal_part = data[8]
        temperature_decimal_value = (temp_byte_decimal_part & 0x0F) / 10.0

        temperature_value = temp_byte & 0x7F
        temp_sign = 1 if (temp_byte & 0x80) else -1

        temperature = float((temperature_value + temperature_decimal_value) * temp_sign)

        # Humidity parsing: Lower 7 bits for the percentage (0-100%).
        humidity = humidity_byte & 0x7F

        return {
            "temperature_celsius": round(temperature, 2),
            "humidity_percent": humidity
        }

    except IndexError:
        print("[ERROR] Insufficient data bytes for temperature/humidity calculation in new beacon parser.")
        return None


def parse_legacy_data(data: bytes):
    if len(data) < 11:
        print(f"[ERROR] Invalid manufacturer data length for legacy parser: {len(data)} bytes. Expected at least 11.")
        return None
    
    try:
        sign = data[9] & 0b10000000
        temperature_decimals = data[8] & 0b00001111
        temperature = (data[9] & 0b01111111)
        if sign == 0: # This means if MSB is 0, it's negative. This is unusual.
            temperature = -temperature
        humidity = data[10] & 0b01111111
        temperature_float = float(f"{temperature}.{temperature_decimals}")
        return {
            "temperature_celsius": temperature_float,
            "humidity_percent": humidity
        }
    except IndexError:
        print("[ERROR] Insufficient data bytes for temperature/humidity calculation in legacy parser.")
        return None


def handle_advertisement(device, advertisement_data):
    mac = device.address.upper()
    # print(f"[DEBUG] Advertisement received from {mac}")

    if mac not in TARGET_MAC_ADDRESSES:
        return

    # Safely get manufacturer data
    manufacturer_data = advertisement_data.manufacturer_data.get(MANUFACTURER_ID)
    
    if not manufacturer_data:
        print(f"[DEBUG] No manufacturer data ({hex(MANUFACTURER_ID)}) for {mac}. Skipping.")
        return

    print(f"[DEBUG] Manufacturer data for {mac}: {manufacturer_data.hex()}")

    parsed = None
    if mac == "D4:35:34:35:68:4D":
        print(f"[DEBUG] Using legacy parser for {mac}")
        parsed = parse_legacy_data(manufacturer_data)
    elif mac == "F2:B2:02:06:7C:49":
        print(f"[DEBUG] Using beacon parser for {mac}")
        parsed = parse_beacon_data(manufacturer_data)
    else:
        print(f"[WARN] No parser defined for {mac}. This should not happen for target MACs.")
        return

    if not parsed:
        print(f"[WARN] Could not parse data for {mac}. Data: {manufacturer_data.hex()}")
        return

    temperature = parsed.get('temperature_celsius')
    humidity = parsed.get('humidity_percent')

    if temperature is not None and humidity is not None:
        print(f"[{mac}] Temperature: {temperature}°C, Humidity: {humidity}%")
        write_metrics(mac, temperature, humidity)
    elif temperature is not None:
        print(f"[{mac}] Temperature: {temperature}°C (Humidity data not available)")
    else:
        print(f"[WARN] Parsed data for {mac} is incomplete. Parsed: {parsed}")


async def scan_ble():
    print(f"[INFO] Scanning for BLE devices: {', '.join(TARGET_MAC_ADDRESSES)}...")
    scanner = BleakScanner(handle_advertisement)
    try:
        await scanner.start()
        await asyncio.sleep(5) # Scan duration
        await scanner.stop()
    except Exception as e:
        print(f"[ERROR] Error during BLE scan: {e}")
    finally:
        print("[INFO] Scan complete.")


async def main():
    start_http_server(PROMETHEUS_PORT, registry=registry)
    print(f"[INFO] Prometheus metrics server started on port {PROMETHEUS_PORT}")
    print(f"[INFO] Metrics available at: http://localhost:{PROMETHEUS_PORT}/metrics")

    while True:
        await scan_ble()
        await asyncio.sleep(55) # Wait for 55 seconds before the next scan, total 60s cycle

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[INFO] Script terminated by user (Ctrl+C).")
    except Exception as e:
        print(f"[CRITICAL] An unhandled error occurred: {e}")