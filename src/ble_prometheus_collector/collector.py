import asyncio
import logging
import time

from prometheus_client import Gauge, CollectorRegistry, start_http_server
from bleak import BleakScanner

from .parsers import parse_beacon_data, parse_legacy_data
from .settings import LEGACY_MAC_ADDRESSES, TARGET_MAC_ADDRESSES

PROMETHEUS_PORT = 8000
MANUFACTURER_ID = 2409

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

registry = CollectorRegistry()
temperature_gauge = Gauge(
    "ble_temperature_celsius",
    "Temperature from BLE beacon in Celsius",
    ["device_address"],
    registry=registry,
)
humidity_gauge = Gauge(
    "ble_humidity_percent",
    "Humidity from BLE beacon in percent",
    ["device_address"],
    registry=registry,
)
last_update_gauge = Gauge(
    "ble_last_update_timestamp",
    "Last update timestamp from BLE beacon",
    ["device_address"],
    registry=registry,
)


def write_metrics(device_address, temperature_float, humidity):
    now = time.time()
    logger.info(
        "Writing metrics for %s: temp=%s°C, hum=%s%%, ts=%s",
        device_address,
        temperature_float,
        humidity,
        now,
    )
    temperature_gauge.labels(device_address=device_address).set(temperature_float)
    humidity_gauge.labels(device_address=device_address).set(humidity)
    last_update_gauge.labels(device_address=device_address).set(now)


def handle_advertisement(device, advertisement_data):
    mac = device.address.upper()
    # print(f"[DEBUG] Advertisement received from {mac}")

    if mac not in TARGET_MAC_ADDRESSES:
        return

    # Safely get manufacturer data
    manufacturer_data = advertisement_data.manufacturer_data.get(MANUFACTURER_ID)

    if not manufacturer_data:
        logger.debug(
            "No manufacturer data (%s) for %s. Skipping.", hex(MANUFACTURER_ID), mac
        )
        return

    logger.debug("Manufacturer data for %s: %s", mac, manufacturer_data.hex())

    parsed = None
    if mac in LEGACY_MAC_ADDRESSES:
        logger.debug("Using legacy parser for %s", mac)
        parsed = parse_legacy_data(manufacturer_data)
    else:
        logger.debug("Using beacon parser for %s", mac)
        parsed = parse_beacon_data(manufacturer_data)

    if not parsed:
        logger.warning(
            "Could not parse data for %s. Data: %s", mac, manufacturer_data.hex()
        )
        return

    temperature = parsed.get("temperature_celsius")
    humidity = parsed.get("humidity_percent")

    if temperature is not None and humidity is not None:
        logger.info(
            "[%s] Temperature: %s°C, Humidity: %s%%", mac, temperature, humidity
        )
        write_metrics(mac, temperature, humidity)
    elif temperature is not None:
        logger.info(
            "[%s] Temperature: %s°C (Humidity data not available)", mac, temperature
        )
    else:
        logger.warning("Parsed data for %s is incomplete. Parsed: %s", mac, parsed)


async def scan_ble():
    logger.info("Scanning for BLE devices: %s...", ", ".join(TARGET_MAC_ADDRESSES))
    scanner = BleakScanner(handle_advertisement)
    try:
        await scanner.start()
        await asyncio.sleep(5)  # Scan duration
        await scanner.stop()
    except Exception as e:
        logger.exception("Error during BLE scan: %s", e)
    finally:
        logger.info("Scan complete.")


async def main():
    try:
        start_http_server(PROMETHEUS_PORT, registry=registry)
    except Exception:
        logger.exception(
            "Failed to start Prometheus metrics server on port %s",
            PROMETHEUS_PORT,
        )
        raise

    logger.info("Prometheus metrics server started on port %s", PROMETHEUS_PORT)
    logger.info("Metrics available at: http://localhost:%s/metrics", PROMETHEUS_PORT)

    while True:
        await scan_ble()
        await asyncio.sleep(
            55
        )  # Wait for 55 seconds before the next scan, total 60s cycle


def main_sync():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script terminated by user (Ctrl+C).")
    except Exception:
        # Keep the non-zero exit status so Kubernetes can detect and restart a
        # collector that never opened its metrics endpoint.
        logger.critical("Collector stopped due to an unhandled error", exc_info=True)
        raise


if __name__ == "__main__":
    main_sync()
