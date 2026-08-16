"""Parsers for SwitchBot BLE manufacturer data."""


def parse_beacon_data(data: bytes) -> dict[str, float | int] | None:
    """Parse the current beacon payload format."""
    if len(data) < 11:
        return None

    decimal = (data[8] & 0x0F) / 10.0
    temperature = (data[9] & 0x7F) + decimal
    if not data[9] & 0x80:
        temperature = -temperature

    return {
        "temperature_celsius": round(temperature, 2),
        "humidity_percent": data[10] & 0x7F,
    }


def parse_legacy_data(data: bytes) -> dict[str, float | int] | None:
    """Parse the legacy beacon payload format."""
    if len(data) < 11:
        return None

    temperature = data[9] & 0x7F
    if not data[9] & 0x80:
        temperature = -temperature

    decimal = data[8] & 0x0F
    return {
        "temperature_celsius": float(f"{temperature}.{decimal}"),
        "humidity_percent": data[10] & 0x7F,
    }
