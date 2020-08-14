from asyncio import Task
from typing import AsyncGenerator, Callable, Dict, Generic, Tuple, TypeVar


class Status:
    "Convert asyncio Task to bluesky Status interface"
    def __init__(self, task: Task):
        self._task = task
        self._callbacks = []
        self._task.add_done_callback(self._run_callbacks)

    @property
    def done(self):
        return self._task.done()

    @property
    def success(self):
        assert self.done, "Status has not completed yet"
        try:
            self._task.result()
        except Exception:
            return False
        else:
            return True

    def add_callback(self, callback: Callable[["Status"], None]):
        if self.done:
            callback(self)
        else:
            self._callbacks.append(callback)

    def _run_callbacks(self, task: Task):
        for callback in self._callbacks:
            callback(self)


class ReadableDevice:
    "Minimal things for a Device that can be read and paused"
    def __init__(self, name: str, parent=None):
        self.name = name
        self.parent = parent

    def read(self) -> dict:
        raise NotImplementedError(self)

    def describe(self) -> dict:
        raise NotImplementedError(self)

    def trigger(self) -> Status:
        raise NotImplementedError(self)

    def read_configuration(self) -> dict:
        return {}

    def describe_configuration(self) -> dict:
        return {}

    def configure(self, *args, **kwargs) -> Tuple[Dict, Dict]:
        raise NotImplementedError(self)

    def pause(self):
        raise NotImplementedError(self)

    def resume(self):
        raise NotImplementedError(self)


T = TypeVar("T")


class CanRead(Generic[T]):
    @property
    async def value(self) -> T:
        raise NotImplementedError(self)

    @property
    async def values(self) -> AsyncGenerator[T, None]:
        raise NotImplementedError(self)


class CanWrite(Generic[T]):
    async def put(self, value: T) -> T:
        raise NotImplementedError(self)


class ChannelRO(CanRead[T]):
    pass


class ChannelRW(CanRead[T], CanWrite[T]):
    pass


class ChannelWO(CanWrite[T]):
    pass
