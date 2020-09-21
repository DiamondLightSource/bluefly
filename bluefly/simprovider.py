from __future__ import annotations

import asyncio
import collections.abc
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, Mapping, TypeVar

from bluesky.run_engine import get_bluesky_event_loop

from .core import (
    AwaitableSignals,
    Signal,
    SignalDetails,
    SignalProvider,
    SignalR,
    SignalRW,
    SignalW,
    SignalX,
    Status,
    ValueT,
)

SetCallback = Callable[[Any], Awaitable[None]]
CallCallback = Callable[[], Awaitable[None]]


class _SimStore:
    def __init__(self):
        self.on_set: Dict[int, SetCallback] = {}
        self.on_call: Dict[int, CallCallback] = {}
        self.values: Dict[int, Any] = {}
        self.events: Dict[int, asyncio.Event] = {}

    def set_value(self, signal_id: int, value):
        self.values[signal_id] = value
        self.events[signal_id].set()
        self.events[signal_id] = asyncio.Event()


class SimSignal(Signal):
    def __init__(self, source: str, store: _SimStore):
        self.source = source
        self._store = store

    async def connected(self) -> bool:
        return True


class SimSignalR(SignalR[ValueT], SimSignal):
    async def get(self) -> ValueT:
        return self._store.values[id(self)]

    async def observe(self) -> AsyncGenerator[ValueT, None]:
        id_self = id(self)
        while True:
            yield self._store.values[id_self]
            await self._store.events[id_self].wait()


class SimSignalW(SignalW[ValueT], SimSignal):
    """Signal that can be put to"""

    async def _do_set(self, value):
        id_self = id(self)
        cb = self._store.on_set.get(id_self, None)
        if cb:
            await cb(value)
        self._store.set_value(id_self, value)
        return value

    def set(self, value: ValueT) -> Status[ValueT]:
        return Status(self._do_set(value))


class SimSignalRW(SimSignalR[ValueT], SimSignalW[ValueT], SignalRW[ValueT]):
    pass


class SimSignalX(SignalX, SimSignal):
    async def __call__(self):
        cb = self._store.on_call.get(id(self), None)
        if cb:
            await cb()


lookup = {
    SignalR: SimSignalR,
    SignalW: SimSignalW,
    SignalRW: SimSignalRW,
    SignalX: SimSignalX,
}


SetCallbackT = TypeVar("SetCallbackT", bound=SetCallback)
CallCallbackT = TypeVar("CallCallbackT", bound=CallCallback)


class SimProvider(SignalProvider):
    def __init__(self):
        self._store = _SimStore()

    def on_set(self, signal: SignalW) -> Callable[[SetCallbackT], SetCallbackT]:
        def decorator(cb: SetCallbackT) -> SetCallbackT:
            self._store.on_set[id(signal)] = cb
            return cb

        return decorator

    def on_call(self, signal: SignalX) -> Callable[[CallCallbackT], CallCallbackT]:
        def decorator(cb: CallCallbackT) -> CallCallbackT:
            self._store.on_call[id(signal)] = cb
            return cb

        return decorator

    def get_value(self, signal: SignalR[ValueT]) -> ValueT:
        return self._store.values[id(signal)]

    def set_value(self, signal: SignalR[ValueT], value: ValueT) -> ValueT:
        self._store.set_value(id(signal), value)
        return value

    async def _connect_signals(
        self, box_id: str, details: Dict[str, SignalDetails]
    ) -> Mapping[str, Signal]:
        # In CA, this would be replaced with caget of {box_id}PVI,
        # then use the json contained in it to connect to all the
        # channels
        signals = {}
        for attr_name, d in details.items():
            signal_cls = lookup[d.signal_cls]
            signal = signal_cls(f"{box_id}.{attr_name}", self._store)
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
                self._store.values[id(signal)] = value
                self._store.events[id(signal)] = asyncio.Event(
                    loop=get_bluesky_event_loop()
                )
            signals[attr_name] = signal
        return signals

    def make_signals(
        self,
        box_id: str,
        details: Dict[str, SignalDetails] = {},
        add_extra_signals=False,
    ) -> AwaitableSignals:
        """For each signal detail listed in details, make a Signal of the given
        base_class. If add_extra_signals then include signals not listed in details.
        AttrName will be mapped to attr_name. Return {attr_name: signal}"""
        return self._connect_signals(box_id, details)
