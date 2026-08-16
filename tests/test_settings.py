import pytest

from ble_prometheus_collector.settings import (
    DEFAULT_LEGACY_MAC_ADDRESSES,
    DEFAULT_TARGET_MAC_ADDRESSES,
    parse_legacy_mac_addresses,
    parse_target_mac_addresses,
)


def test_uses_defaults_when_environment_variable_is_unset():
    assert parse_target_mac_addresses(None) == DEFAULT_TARGET_MAC_ADDRESSES


def test_normalizes_and_deduplicates_injected_addresses():
    value = " aa:bb:cc:dd:ee:ff,11:22:33:44:55:66,AA:BB:CC:DD:EE:FF "

    assert parse_target_mac_addresses(value) == (
        "AA:BB:CC:DD:EE:FF",
        "11:22:33:44:55:66",
    )


@pytest.mark.parametrize("value", ["", "not-a-mac", "AA:BB:CC:DD:EE"])
def test_rejects_empty_or_invalid_addresses(value):
    with pytest.raises(ValueError, match="TARGET_MAC_ADDRESSES|Invalid MAC address"):
        parse_target_mac_addresses(value)


def test_uses_legacy_default_when_it_is_a_target():
    assert (
        parse_legacy_mac_addresses(None, DEFAULT_TARGET_MAC_ADDRESSES)
        == DEFAULT_LEGACY_MAC_ADDRESSES
    )


def test_custom_targets_default_to_non_legacy():
    assert parse_legacy_mac_addresses(None, ("AA:BB:CC:DD:EE:FF",)) == ()


def test_injects_legacy_addresses_and_allows_none():
    targets = ("AA:BB:CC:DD:EE:FF", "11:22:33:44:55:66")

    assert parse_legacy_mac_addresses("aa:bb:cc:dd:ee:ff", targets) == (
        "AA:BB:CC:DD:EE:FF",
    )
    assert parse_legacy_mac_addresses("", targets) == ()


def test_rejects_legacy_address_that_is_not_a_target():
    with pytest.raises(ValueError, match="must be included in TARGET_MAC_ADDRESSES"):
        parse_legacy_mac_addresses("11:22:33:44:55:66", ("AA:BB:CC:DD:EE:FF",))
