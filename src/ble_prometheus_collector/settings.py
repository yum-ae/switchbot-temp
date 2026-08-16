"""Runtime configuration loaded from environment variables."""

import os
import re

DEFAULT_TARGET_MAC_ADDRESSES = (
    "D4:35:34:35:68:4D",
    "F2:B2:02:06:7C:49",
)
DEFAULT_LEGACY_MAC_ADDRESSES = ("D4:35:34:35:68:4D",)
MAC_ADDRESS_PATTERN = re.compile(r"^(?:[0-9A-F]{2}:){5}[0-9A-F]{2}$")


def _parse_mac_addresses(
    value: str | None,
    *,
    variable_name: str,
    default: tuple[str, ...],
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if value is None:
        return default

    addresses = tuple(
        dict.fromkeys(address.strip().upper() for address in value.split(",") if address.strip())
    )
    if not addresses and not allow_empty:
        raise ValueError(f"{variable_name} must contain at least one MAC address")

    invalid_addresses = [address for address in addresses if not MAC_ADDRESS_PATTERN.fullmatch(address)]
    if invalid_addresses:
        raise ValueError(f"Invalid MAC address in {variable_name}: " + ", ".join(invalid_addresses))

    return addresses


def parse_target_mac_addresses(value: str | None) -> tuple[str, ...]:
    """Parse the comma-separated target device list."""
    return _parse_mac_addresses(
        value,
        variable_name="TARGET_MAC_ADDRESSES",
        default=DEFAULT_TARGET_MAC_ADDRESSES,
    )


def parse_legacy_mac_addresses(
    value: str | None, target_mac_addresses: tuple[str, ...]
) -> tuple[str, ...]:
    """Parse the legacy device list and require it to be a target subset."""
    default = tuple(address for address in DEFAULT_LEGACY_MAC_ADDRESSES if address in target_mac_addresses)
    addresses = _parse_mac_addresses(
        value,
        variable_name="LEGACY_MAC_ADDRESSES",
        default=default,
        allow_empty=True,
    )

    unknown_addresses = [address for address in addresses if address not in target_mac_addresses]
    if unknown_addresses:
        raise ValueError(
            "LEGACY_MAC_ADDRESSES must be included in TARGET_MAC_ADDRESSES: "
            + ", ".join(unknown_addresses)
        )

    return addresses


TARGET_MAC_ADDRESSES = parse_target_mac_addresses(os.getenv("TARGET_MAC_ADDRESSES"))
LEGACY_MAC_ADDRESSES = frozenset(
    parse_legacy_mac_addresses(os.getenv("LEGACY_MAC_ADDRESSES"), TARGET_MAC_ADDRESSES)
)
