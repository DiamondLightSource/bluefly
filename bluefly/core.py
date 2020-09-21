import asyncio
import logging
import os
import sys
import threading
from abc import ABC, abstractmethod
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
    cast,
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
            logging.exception("Failed status")
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


class Signal(ABC):
    """Signals are like ophyd Signals, but async"""

    source: str  # like ca://PV_PREFIX:SIGNAL or panda://172.23.252.201/PCAP/ARM

    @abstractmethod
    async def connected(self) -> bool:
        """Has the connection to the control system been successful?"""

    __lt__ = __le__ = __eq__ = __ge__ = __gt__ = __ne__ = _fail


class SignalR(Signal, Generic[ValueT]):
    """Signal that can be read from and monitored"""

    @abstractmethod
    async def get(self) -> ValueT:
        """The current value"""

    @abstractmethod
    async def observe(self) -> AsyncGenerator[ValueT, None]:
        """Observe changes to the current value. First update is the
        current value"""
        return
        yield


class SignalW(Signal, Generic[ValueT]):
    """Signal that can be put to"""

    @abstractmethod
    def set(self, value: ValueT) -> Status[ValueT]:
        """Send the value to the control system, returning a Status
        to show when it is done"""


class SignalRW(SignalR[ValueT], SignalW[ValueT]):
    """Signal that can be read from, monitored, and put to"""


class SignalX(Signal):
    """Signal that can be executed"""

    @abstractmethod
    async def __call__(self):
        """Execute this"""


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


class SignalProvider(ABC):
    @abstractmethod
    def make_signals(
        self,
        signal_prefix: str,
        details: Dict[str, SignalDetails] = {},
        add_extra_signals=False,
    ) -> AwaitableSignals:
        """For each signal detail listed in details, make a Signal of the given
        base_class. If add_extra_signals then include signals not listed in details.
        AttrName will be mapped to attr_name. Return {attr_name: signal}"""


class HasSignals:
    # obj id, like ca://BLxxI-MO-PMAC-01: or panda://172.23.252.201:8888/
    signal_prefix: str

    def __init__(self, signal_prefix: str):
        self.signal_prefix = signal_prefix
        SignalCollector.make_signals(self, add_extra_signals=True)


SignalProviderT = TypeVar("SignalProviderT", bound=SignalProvider)


InstanceT = TypeVar("InstanceT", bound="_SingletonContextManager")


class _SingletonContextManager:
    """Pattern where instance exists only during the context manager. Works with
    both async and regular context manager invocations"""

    _instance: ClassVar[Optional["_SingletonContextManager"]] = None

    @classmethod
    def _get_cls(cls):
        return cls

    def __enter__(self):
        cls = self._get_cls()
        assert not cls._instance, f"Can't nest {cls} context managers"
        cls._instance = self
        return self

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, type_, value, traceback):
        self.__exit__(type_, value, traceback)

    def __exit__(self, type_, value, traceback):
        self._get_cls()._instance = None

    @classmethod
    def get_instance(cls: Type[InstanceT]) -> InstanceT:
        assert (
            cls._instance
        ), f"Can only call classmethods of {cls} within a contextmanager"
        return cast(InstanceT, cls._instance)


class SignalCollector(_SingletonContextManager):
    """Collector of Signals from HasSignals instances to be used as a context manager:

    [async] with SignalCollector():
        SignalCollector.add_provider(ca=CAProvider(), set_default=True)
        t1x = SettableMotor(MotorRecord("BLxxI-MO-TABLE-01:X"))
        t1y = SettableMotor(MotorRecord("BLxxI-MO-TABLE-01:Y"))
        # All Signals get connected at the end of the Context
    assert t1x.motor.velocity.connected
    """

    def __init__(self):
        self._providers: Dict[str, SignalProvider] = {}
        self._object_signals: Dict[HasSignals, AwaitableSignals] = {}

    async def __aexit__(self, type_, value, traceback):
        self._get_cls()._instance = None
        # Populate all the Signals
        awaitables = (self._populate_signals(obj) for obj in self._object_signals)
        await asyncio.gather(*awaitables)

    def __exit__(self, type_, value, traceback):
        fut = asyncio.run_coroutine_threadsafe(
            self.__aexit__(type_, value, traceback), loop=get_bluesky_event_loop(),
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
    def add_provider(
        cls, set_default: bool = False, **providers: SignalProviderT
    ) -> SignalProviderT:
        self = cls.get_instance()
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
            self = cls.get_instance()
            split = obj.signal_prefix.split("://", 1)
            if len(split) > 1:
                transport, signal_prefix = split
            else:
                transport, signal_prefix = "", split[0]
            provider = self._providers[transport]
            self._object_signals[obj] = provider.make_signals(
                signal_prefix, details, add_extra_signals
            )


class NamedDevices:
    """Context manager that names Devices after their name in locals().

    [async] with NamedDevices():
        t1x = SettableMotor(MotorRecord("BLxxI-MO-TABLE-01:X"))
    assert t1x.name == "t1x"
    """

    def __init__(self):
        self._names_on_enter: Set[str] = set()

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
        # Stash the names that were defined before we were called
        self._names_on_enter = set(self._caller_locals())
        return self

    async def __aenter__(self):
        return self.__enter__()

    def __exit__(self, type, value, traceback):
        for name, obj in self._caller_locals().items():
            if name not in self._names_on_enter and isinstance(obj, Device):
                # We got a device, name it
                obj.name = name

    async def __aexit__(self, type, value, traceback):
        return self.__exit__(type, value, traceback)


ConfigDict = Dict[str, Dict[str, Any]]


class Device:
    # Human readable name, as required by Bluesky, set by SignalCollector
    name: Optional[str] = None
    parent = None

    def configure(self, d: Dict[str, Any]) -> Tuple[ConfigDict, ConfigDict]:
        return ({}, {})

    def read_configuration(self) -> ConfigDict:
        return {}

    def describe_configuration(self) -> ConfigDict:
        return {}

    # Can also add stage, unstage, pause, resume


class ReadableDevice(Device):
    def read(self) -> ConfigDict:
        return {}

    def describe(self) -> ConfigDict:
        return {}

    def trigger(self) -> Status:
        raise NotImplementedError(self)


class SettableDevice(ReadableDevice):
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


class FilenameScheme(_SingletonContextManager, ABC):
    """Filenaming scheme for detectors where naming matters:

    [async] with MyFilenameScheme():
        det = DetectorDriver(...)

    det.stage()
    det.trigger()
    # det has called current_prefix() on the FilenameScheme instance
    """

    _current_prefix: Optional[str] = None
    _using_current_prefix: bool = False

    @classmethod
    def _get_cls(cls):
        # Always store the instance on the base FilenameScheme, so detectors
        # can get their singleton scheme
        return FilenameScheme

    async def current_prefix(self) -> str:
        # Mark someone as using it, this will be called at stage
        self._using_current_prefix = True
        if self._current_prefix is None:
            self._current_prefix = await self._generate_prefix()
        return self._current_prefix

    async def done_using_prefix(self):
        # Someone has finished using it, this will be called at stage
        if self._using_current_prefix:
            # The first time someone finished using it, make a new one
            # Subsequent times will not make a new one
            self._current_prefix = await self._generate_prefix()
            self._using_current_prefix = False

    @abstractmethod
    async def _generate_prefix(self) -> str:
        """Implement this to make a new file prefix for a scan"""


class TmpFilenameScheme(FilenameScheme):
    """Filenaming scheme that makes a temporary directory on each run"""

    async def _generate_prefix(self) -> str:
        return mkdtemp() + os.sep


@dataclass
class HDFDatasetResource:
    """Metadata about an interesting dataset in an HDF file"""

    name: str = "data"
    dataset_path: str = "/entry/data/data"


@dataclass
class HDFResource:
    """Describe the path, spec, interesting data, and summary dataset of an HDF file"""

    data: List[HDFDatasetResource]
    summary: HDFDatasetResource
    file_path: str
    # AD_HDF5 means primary dataset must be /entry/data/data
    spec: str = "AD_HDF5"
