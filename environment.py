import asyncio
import binascii
import datetime
from bleak import BleakScanner
from elasticsearch import AsyncElasticsearch

TARGET_MAC_ADDRESS = "D4:35:"
MANUFACTURER_ID = 0x2409
ELASTICSEARCH_HOST = "https://localhost:9200"
INDEX_NAME = "temperature_humidity"

es = AsyncElasticsearch(
        hosts=[ELASTICSEARCH_HOST],
        http_auth=('elastic','xxxxxxxxxxxxxxxxxx'),
        verify_certs=False
    )
es.info()

def parse_temperature_humidity(data: bytes):
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
    temperature_str = f"{temperature}.{temperature_decimals}"
    temperature_float = float(temperature_str)

    asyncio.create_task(send_to_elasticsearch(TARGET_MAC_ADDRESS, temperature_float, humidity))


async def send_to_elasticsearch(device_address: str, temperature: float, humidity: int):
    timestamp = datetime.datetime.utcnow().isoformat()
    document = {
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
        "device_address": device_address
    }
    await es.index(index=INDEX_NAME, document=document)
    print("Data sent to Elasticsearch")


async def scan_ble():
    def callback(device, advertisement_data):
        if device.address.upper() == TARGET_MAC_ADDRESS:
            # print(f"Device Found: {device.name} ({device.address}), RSSI: {device.rssi}")

            manufacturer_data = advertisement_data.manufacturer_data[2409]
            if manufacturer_data:
                # print(f"Raw Manufacturer Data: {binascii.hexlify(manufacturer_data).decode()}")
                parse_temperature_humidity(manufacturer_data)

    print(f"Scanning for BLE device with MAC address: {TARGET_MAC_ADDRESS}...")
    scanner = BleakScanner(callback)
    await scanner.start()
    await asyncio.sleep(5)  # 5sec scan
    await scanner.stop()
    print("Scan complete.")

async def main():
    while True:
        await scan_ble()
        await asyncio.sleep(55)
    await es.close()

asyncio.run(main())