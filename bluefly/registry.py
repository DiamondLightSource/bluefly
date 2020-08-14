"""Fake up something that could make Devices from GraphQL connection to coniql"""

from typing import Protocol, TypeVar, cast

registry = {}

TProtocol = TypeVar("TProtocol", bound=Protocol)


def make_device(id: str, protocol: TProtocol) -> TProtocol:
    # Here we would connect to coniql, get the structure of the
    # Device, and create a device with fields described in the given
    # protocol
    device = registry[id]
    for k in protocol:
        assert hasattr(device, k), f"Device hasn't got field {k}"
    return cast(TProtocol, device)


