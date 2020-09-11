import asyncio
import os
import sys
import threading
from asyncio import Task
from dataclasses import dataclass
from tempfile import mkdtemp
from typing import (
    Any,
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
    Tuple,
    Type,
    TypeVar,
    Union,
    get_type_hints,
)

from bluesky.run_engine import get_bluesky_event_loop
from scanpointgenerator import CompoundGenerator
from scanpointgenerator.core.point import Point, Points

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


def _fail(*args, **kwargs):
    raise ValueError(
        "Can't compare two Signals, did you mean await signal.get() instead?"
    )


class Signal:
    """Signals are like ophyd Signals, but async"""

    source: str  # like ca://PV_PREFIX:SIGNAL or panda://172.23.252.201/PCAP/ARM

    async def connected(self) -> bool:
        raise NotImplementedError(self)

    __lt__ = __le__ = __eq__ = __ge__ = __gt__ = __ne__ = _fail


class SignalR(Signal, Generic[ValueT]):
    """Signal that can be read from and monitored"""

    async def get(self) -> ValueT:
        # TODO: could make this not async, but that would mean all signals
        # would always be monitored, which is bad for performance
        raise NotImplementedError(self)

    async def observe(self, timeout=None) -> AsyncGenerator[ValueT, None]:
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


class NotConnectedError(Exception):
    """Raised when a Signal get, set, observe or call happens and it is not connected"""


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


AwaitableSignals = Awaitable[Mapping[str, Signal]]


class SignalProvider:
    def make_signals(
        self,
        signal_prefix: str,
        details: Dict[str, SignalDetails] = {},
        add_extra_signals=False,
    ) -> AwaitableSignals:
        """For each signal detail listed in details, make a Signal of the given
        base_class. If add_extra_signals then include signals not listed in details.
        AttrName will be mapped to attr_name. Return {attr_name: signal}"""
        raise NotImplementedError(self)


class HasSignals:
    # obj id, like ca://BLxxI-MO-PMAC-01: or panda://172.23.252.201:8888/
    signal_prefix: str

    def __init__(self, signal_prefix: str):
        self.signal_prefix = signal_prefix
        SignalCollector.make_signals(self, add_extra_signals=True)


SignalProviderT = TypeVar("SignalProviderT", bound=SignalProvider)


class SignalCollector:
    """Collector of Signals from Signalobjes to be used as a context manager:

    with SignalCollector() as sc:
        sc.add_provider(ca=CAProvider(), set_default=True)
        t1x = SettableMotor(MotorRecord("BLxxI-MO-TABLE-01:X"))
        t1y = SettableMotor(MotorRecord("BLxxI-MO-TABLE-01:Y"))
        pmac1 = PMAC("BLxxI-MO-PMAC-01")
        im = PCODetector("BLxxI-EA-DET-01")
        diff = PilatusDetector("BLxxI-EA-DET-02")
        tomo_scan = FlyDevice(
            PmacMasterFlyLogic(pmac=pmac1, motors=[t1x, t1y], detectors=[im, diff])
        )
        # Signals get connected and Devices get named at the end of the Context
    assert t1x.name == "t1x"
    assert t1x.motor.velocity.connected
    """

    _instance: ClassVar[Optional["SignalCollector"]] = None

    def __init__(self):
        self._providers: Dict[str, SignalProvider] = {}
        self._names_on_enter: Set[str] = set()
        self._object_signals: Dict[HasSignals, AwaitableSignals] = {}

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
        # Populate all the Signalobjes
        awaitables = (self._populate_signals(obj) for obj in self._object_signals)
        await asyncio.gather(*awaitables)

    def __exit__(self, type, value, traceback):
        fut = asyncio.run_coroutine_threadsafe(
            self.__aexit__(type, value, traceback, self._caller_locals()),
            loop=get_bluesky_event_loop(),
        )
        event = threading.Event()
        fut.add_done_callback(lambda _: event.set())
        event.wait()

    async def _populate_signals(self, obj: HasSignals):
        signals = await self._object_signals[obj]
        for attr_name, signal in signals.items():
            assert not hasattr(
                obj, attr_name
            ), f"{obj} already has attr {attr_name}: {getattr(obj, attr_name)}"
            setattr(obj, attr_name, signal)
        return obj

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
    def make_signals(cls, obj: HasSignals, add_extra_signals: bool):
        # Make channel details from the type
        hints = get_type_hints(type(obj))  # type: ignore
        signal_sources = getattr(obj, "__signal_sources__", {})
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
            split = obj.signal_prefix.split("://", 1)
            if len(split) > 1:
                transport, signal_prefix = split
            else:
                transport, signal_prefix = "", split[0]
            provider = self._providers[transport]
            self._object_signals[obj] = provider.make_signals(
                signal_prefix, details, add_extra_signals
            )


ConfigDict = Dict[str, Dict[str, Any]]


class Device:
    # Human readable name, as required by Bluesky, set by SignalCollector
    name: Optional[str] = None
    parent = None

    def configure(self, d: Dict[str, Any]) -> Tuple[ConfigDict, ConfigDict]:
        pass

    def read_configuration(self) -> ConfigDict:
        return {}

    def describe_configuration(self) -> ConfigDict:
        return {}

    def read(self) -> ConfigDict:
        return {}

    def describe(self) -> ConfigDict:
        return {}

    def trigger(self) -> Status:
        status = Status(asyncio.sleep(0))
        return status

    # Can also add stage, unstage, pause, resume


class SettableDevice(Device):
    def set(self, new_position: float, timeout: float = None) -> Status[float]:
        raise NotImplementedError(self)

    def stop(self, *, success=False):
        raise NotImplementedError(self)


class RemainingPoints:
    """CompoundGenerator prepared with progress indicator"""

    def __init__(self, spg: CompoundGenerator, completed: int):
        self.spg = spg
        self.completed = completed

    def peek_point(self) -> Point:
        return self.spg.get_point(self.completed)

    def get_points(self, num) -> Points:
        new_completed = min(self.completed + num, self.spg.size)
        points = self.spg.get_points(self.completed, new_completed)
        self.completed = new_completed
        return points

    @property
    def constant_duration(self) -> float:
        assert self.spg.duration, "Scan point generator has variable duration"
        return self.spg.duration

    @property
    def remaining(self) -> int:
        return self.spg.size - self.completed

    @property
    def size(self) -> int:
        return self.spg.size


@dataclass
class FileDetails:
    file_path: str
    file_template: str
    file_name: str

    def full_path(self):
        # TODO: this need windows/linux path conversion, etc.
        return self.file_template % (self.file_path, self.file_name)


class FilenameScheme:
    file_path: Optional[str] = None
    file_template: str = "%s%s.h5"

    async def new_scan(self):
        self.file_path = mkdtemp() + os.sep


@dataclass
class DatasetDetails:
    data_shape: Tuple[int, ...]  # Fastest moving last, e.g. (768, 1024)
    data_suffix: str = "_data"
    data_path: str = "/entry/data/data"
    summary_suffix: str = "_sum"
    summary_path: str = "/entry/sum"
