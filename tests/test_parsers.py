from ble_prometheus_collector.parsers import parse_beacon_data, parse_legacy_data


def test_parse_beacon_data_positive_temperature():
    payload = bytes([0] * 8 + [5, 0x80 | 23, 61])

    assert parse_beacon_data(payload) == {
        "temperature_celsius": 23.5,
        "humidity_percent": 61,
    }


def test_parse_beacon_data_negative_temperature():
    payload = bytes([0] * 8 + [2, 4, 50])

    assert parse_beacon_data(payload) == {
        "temperature_celsius": -4.2,
        "humidity_percent": 50,
    }


def test_parse_legacy_data():
    payload = bytes([0] * 8 + [7, 0x80 | 19, 48])

    assert parse_legacy_data(payload) == {
        "temperature_celsius": 19.7,
        "humidity_percent": 48,
    }


def test_short_payloads_are_ignored():
    assert parse_beacon_data(bytes(10)) is None
    assert parse_legacy_data(bytes(10)) is None

