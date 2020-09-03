import asyncio
import sys
import threading
from asyncio import Task
from dataclasses import dataclass
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
    get_type_hints,
)

from bluesky.run_engine import get_bluesky_event_loop


class NotConnectedError(Exception):
    """Raised when a Signal get, set, observe or call happens and it is not connected"""


def _fail(*args, **kwargs):
    raise ValueError(
        "Can't compare two Signals, did you mean await signal.get() instead?"
    )


class Signal:
    """Signals are like ophyd Signals, but async"""

    source: str  # like ca://DEVICE_PREFIX:SIGNAL or panda://172.23.252.201/PCAP/ARM

    async def connected(self) -> bool:
        raise NotImplementedError(self)

    __lt__ = __le__ = __eq__ = __ge__ = __gt__ = __ne__ = _fail


# Would love to write this recursively, but I don't think you can
ValueT = TypeVar(
    "ValueT",
    bool,
    int,
    float,
    str,
    Sequence[bool],
    Sequence[int],
    Sequence[float],
    Sequence[str],
    Dict[str, bool],
    Dict[str, int],
    Dict[str, float],
    Dict[str, str],
    Dict[str, Sequence[bool]],
    Dict[str, Sequence[int]],
    Dict[str, Sequence[float]],
    Dict[str, Sequence[str]],
)

Callback = Callable[["Status"], None]


class Status(Generic[ValueT]):
    "Convert asyncio Task to bluesky Status interface"

    def __init__(
        self,
        awaitable: Awaitable[ValueT],
        add_watcher: Optional[Callable[[Callable], None]] = None,
    ):
        # Note: this doesn't start until we await it or add callback
        self._awaitable: Union[Awaitable[ValueT], Task[ValueT]] = awaitable
        if isinstance(awaitable, Task):
            awaitable.add_done_callback(self._run_callbacks)
        self._callbacks: List[Callback] = []
        self._add_watcher = add_watcher

    def __await__(self):
        yield from self._awaitable.__await__()

    def add_callback(self, callback: Callback):
        if not isinstance(self._awaitable, Task):
            # If it isn't a Task, convert to one here.
            # Don't do this in __init__ as this has a performance hit
            self._awaitable = asyncio.create_task(self._awaitable)
            self._awaitable.add_done_callback(self._run_callbacks)
        if self.done:
            callback(self)
        else:
            self._callbacks.append(callback)

    @property
    def done(self) -> bool:
        assert isinstance(
            self._awaitable, Task
        ), "Can't get done until add_callback is called"
        return self._awaitable.done()

    @property
    def success(self) -> bool:
        assert self.done and isinstance(
            self._awaitable, Task
        ), "Status has not completed yet"
        try:
            self._awaitable.result()
        except Exception:
            # TODO: if we catch CancelledError here we can't resume. Not sure why
            return False
        else:
            return True

    def _run_callbacks(self, task: Task):
        for callback in self._callbacks:
            callback(self)

    def watch(self, watcher: Callable):
        if self._add_watcher:
            self._add_watcher(watcher)


class SignalR(Signal, Generic[ValueT]):
    """Signal that can be read from and monitored"""

    async def get(self) -> ValueT:
        # TODO: could make this not async, but that would mean all signals
        # would always be monitored, which is bad for performance
        raise NotImplementedError(self)

    async def observe(self) -> AsyncGenerator[ValueT, None]:
        raise NotImplementedError(self)
        yield


class SignalW(Signal, Generic[ValueT]):
    """Signal that can be put to"""

    def set(self, value: ValueT) -> Status[ValueT]:
        raise NotImplementedError(self)


class SignalRW(SignalR[ValueT], SignalW[ValueT]):
    """Signal that can be read from, monitored, and put to"""


class SignalX(Signal):
    """Signal that can be executed"""

    async def __call__(self):
        raise NotImplementedError(self)


SignalSource = Union[str, Dict[str, str]]


def signal_sources(**sources: SignalSource) -> Callable[[Type], Type]:
    """Add details about how to map dicts of signals"""

    def decorator(typ: Type) -> Type:
        extra = sorted(set(sources) - set(get_type_hints(typ)))
        assert not extra, f"Signal sources {extra} not described with annotations"
        typ.__signal_sources__ = {**getattr(typ, "__signal_sources__", {}), **sources}
        return typ

    return decorator


@dataclass
class SignalDetails(Generic[ValueT]):
    # Either attr_name or {key: attr_name}
    source: SignalSource
    signal_cls: Type[Signal]
    value_type: Optional[Type[ValueT]] = None

    @classmethod
    def from_annotation(cls, ann, source: SignalSource) -> "SignalDetails":
        # SignalX takes no typevar, so will have no origin
        origin = getattr(ann, "__origin__", ann)
        if not issubclass(origin, Signal):
            raise TypeError(f"Annotation {ann} is not a Signal")
        # This will be [ValueT], or [None] in the case of SignalX
        args = getattr(ann, "__args__", [None])
        details = cls(source=source, signal_cls=origin, value_type=args[0])
        return details


DeviceSignals = Awaitable[Mapping[str, Signal]]


class SignalProvider:
    def make_signals(
        self,
        device_id: str,
        details: Dict[str, SignalDetails] = {},
        add_extra_signals=False,
    ) -> DeviceSignals:
        """For each signal detail listed in details, make a Signal of the given
        base_class. If add_extra_signals then include signals not listed in details.
        AttrName will be mapped to attr_name. Return {attr_name: signal}"""
        raise NotImplementedError(self)


class Device:
    # Human readable name, as required by Bluesky, set by Context
    name: Optional[str] = None
    parent = None


class DeviceWithSignals(Device):
    # Device id, like ca://BLxxI-MO-PMAC-01: or panda://172.23.252.201:8888/
    device_id: str
    # If you want all the Signals that this device has, change this to True
    _add_extra_signals = False

    def __init__(self, device_id: str):
        self.device_id = device_id
        SignalCollector.make_signals(self, self._add_extra_signals)


SignalProviderT = TypeVar("SignalProviderT", bound=SignalProvider)


class SignalCollector:
    """Collector of Signals from Devices to be used as a context manager:

    with SignalCollector() as sc:
        sc.add_provider(ca=CAProvider(), set_default=True)
        sc.add_provider(panda=PandAProvider())

        t1x = EpicsMotor("BLxxI-MO-TABLE-01:X")
        t1y = EpicsMotor("BLxxI-MO-TABLE-01:Y")
        pmac1 = PMAC("BLxxI-MO-PMAC-01")
        im = PCODetector("BLxxI-EA-DET-01")
        diff = PilatusDetector("BLxxI-EA-DET-02")
        tomo_scan = FlyScanDevice(
            PmacMasterLogic(pmac=pmac1, motors=[t1x, t1y], detectors=[im, diff])
        )
    assert t1x.name == "t1x"
    """

    _instance: ClassVar[Optional["SignalCollector"]] = None

    def __init__(self):
        self._providers: Dict[str, SignalProvider] = {}
        self._names_on_enter: Set[str] = set()
        self._device_signals: Dict[DeviceWithSignals, DeviceSignals] = {}

    def _caller_locals(self):
        """Walk up until we find a stack frame that doesn't have us as self"""
        try:
            raise ValueError
        except ValueError:
            _, _, tb = sys.exc_info()
            assert tb, "Can't get traceback, this shouldn't happen"
            caller_frame = tb.tb_frame
            while caller_frame.f_locals.get("self", None) is self:
                caller_frame = caller_frame.f_back
            return caller_frame.f_locals

    def __enter__(self):
        assert (
            not SignalCollector._instance
        ), "Can't nest SignalCollector() context managers"
        SignalCollector._instance = self
        # Stash the names that were defined before we
        self._names_on_enter = set(self._caller_locals())
        return self

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, type, value, traceback, locals_d=None):
        if locals_d is None:
            locals_d = self._caller_locals()
        for name, obj in locals_d.items():
            if name not in self._names_on_enter and isinstance(obj, Device):
                # We got a device, name it
                obj.name = name
        SignalCollector._instance = None
        # Populate all the devices
        awaitables = (self._populate_device(device) for device in self._device_signals)
        await asyncio.gather(*awaitables)

    def __exit__(self, type, value, traceback):
        fut = asyncio.run_coroutine_threadsafe(
            self.__aexit__(type, value, traceback, self._caller_locals()),
            loop=get_bluesky_event_loop(),
        )
        event = threading.Event()
        fut.add_done_callback(lambda _: event.set())
        event.wait()

    async def _populate_device(self, device: DeviceWithSignals):
        signals = await self._device_signals[device]
        for attr_name, signal in signals.items():
            assert not hasattr(
                device, attr_name
            ), f"{device} already has attr {attr_name}: {getattr(device, attr_name)}"
            setattr(device, attr_name, signal)
        return device

    @classmethod
    def _get_current_context(cls) -> "SignalCollector":
        assert cls._instance, "No current context"
        return cls._instance

    def add_provider(
        self, set_default: bool = False, **providers: SignalProviderT
    ) -> SignalProviderT:
        assert len(providers) == 1, "Can only add a single provider"
        transport, provider = list(providers.items())[0]
        self._providers[transport] = provider
        if set_default:
            self._providers[""] = provider
        return provider

    @classmethod
    def make_signals(cls, device: DeviceWithSignals, add_extra_signals: bool):
        # Make channel details from the type
        hints = get_type_hints(type(device))  # type: ignore
        signal_sources = getattr(device, "__signal_sources__", {})
        details: Dict[str, SignalDetails] = {}
        # Look for all attributes with type hints
        for attr_name, ann in hints.items():
            try:
                details[attr_name] = SignalDetails.from_annotation(
                    ann, signal_sources.get(attr_name, attr_name)
                )
            except TypeError:
                # This isn't a Signal, don't make it
                pass

        if details or add_extra_signals:
            self = cls._get_current_context()
            split = device.device_id.split("://", 1)
            if len(split) > 1:
                transport, device_id = split
            else:
                transport, device_id = "", split[0]
            provider = self._providers[transport]
            self._device_signals[device] = provider.make_signals(
                device_id, details, add_extra_signals
            )
