from __future__ import annotations

import asyncio
import collections.abc
from typing import Any, AsyncGenerator, Callable, Dict, Mapping, TypeVar

from bluesky.run_engine import get_bluesky_event_loop

from .core import (
    DeviceSignals,
    Signal,
    SignalDetails,
    SignalProvider,
    SignalR,
    SignalRW,
    SignalW,
    SignalX,
    ValueT,
)


class SimSignal(Signal):
    def __init__(self, provider: SimProvider, source: str):
        self._provider = provider
        self.source = source

    async def connected(self) -> bool:
        return True


class SimSignalR(SignalR[ValueT], SimSignal):
    async def get(self) -> ValueT:
        return await self._provider.get(self)

    async def observe(self) -> AsyncGenerator[ValueT, None]:
        async for v in self._provider.observe(self):
            yield v


class SimSignalW(SignalW[ValueT], SimSignal):
    """Signal that can be put to"""

    async def set(self, value: ValueT) -> ValueT:
        return await self._provider.set(self, value)


class SimSignalRW(SimSignalR[ValueT], SimSignalW[ValueT], SignalRW[ValueT]):
    pass


class SimSignalX(SignalX, SimSignal):
    async def __call__(self):
        await self._provider.call(self)


lookup = {
    SignalR: SimSignalR,
    SignalW: SimSignalW,
    SignalRW: SimSignalRW,
    SignalX: SimSignalX,
}


F = TypeVar("F", bound=Callable[..., Any])


class SimProvider(SignalProvider):
    def __init__(self):
        self._on_set: Dict[int, Callable] = {}
        self._on_call: Dict[int, Callable] = {}
        self._values: Dict[int, Any] = {}
        self._events: Dict[int, asyncio.Event] = {}

    def on_set(self, signal: SignalW) -> Callable[[F], F]:
        def decorator(cb: F) -> F:
            self._on_set[id(signal)] = cb
            return cb

        return decorator

    async def set(self, signal: SignalW, value) -> Any:
        async def default_set(v):
            self._values[id(signal)] = v
            self._events[id(signal)].set()
            self._events[id(signal)] = asyncio.Event()

        await self._on_set.get(id(signal), default_set)(value)
        return value

    def on_call(self, signal: SignalX) -> Callable[[F], F]:
        def decorator(cb: F) -> F:
            self._on_call[id(signal)] = cb
            return cb

        return decorator

    async def call(self, signal):
        async def default_call():
            return

        await self._on_call.get(id(signal), default_call)()

    async def get(self, signal: SignalR):
        return self._values[id(signal)]

    async def observe(self, signal: SignalR):
        while True:
            yield self._values[id(signal)]
            await self._events[id(signal)].wait()

    async def _connect_signals(
        self, device_id: str, details: Dict[str, SignalDetails]
    ) -> Mapping[str, Signal]:
        # In CA, this would be replaced with caget of {device_id}PVI,
        # then use the json contained in it to connect to all the
        # channels
        signals = {}
        for attr_name, d in details.items():
            signal_cls = lookup[d.signal_cls]
            signal = signal_cls(self, f"{device_id}.{attr_name}")
            if isinstance(signal, (SimSignalR, SimSignalW)):
                # Need a value
                origin = getattr(d.value_type, "__origin__", None)
                if origin is None:
                    # str, bool, int, float
                    assert d.value_type
                    value = d.value_type()
                elif origin is collections.abc.Sequence:
                    # Sequence[...]
                    value = ()
                elif origin is dict:
                    # Dict[...]
                    value = origin()
                else:
                    raise ValueError(f"Can't make {d.value_type}")
                self._values[id(signal)] = value
                self._events[id(signal)] = asyncio.Event(loop=get_bluesky_event_loop())
            signals[attr_name] = signal
        return signals

    def make_signals(
        self,
        device_id: str,
        details: Dict[str, SignalDetails] = {},
        add_extra_signals=False,
    ) -> DeviceSignals:
        """For each signal detail listed in details, make a Signal of the given
        base_class. If add_extra_signals then include signals not listed in details.
        AttrName will be mapped to attr_name. Return {attr_name: signal}"""
        return self._connect_signals(device_id, details)
