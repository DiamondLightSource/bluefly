from asyncio import Task
from typing import Callable, List

Callback = Callable[["Status"], None]


class Status:
    "Convert asyncio Task to bluesky Status interface"

    def __init__(self, task: Task, add_watcher: Callable[[Callable], None]):
        self._task = task
        self._callbacks: List[Callback] = []
        self._task.add_done_callback(self._run_callbacks)
        self._add_watcher = add_watcher

    @property
    def done(self) -> bool:
        return self._task.done()

    @property
    def success(self) -> bool:
        assert self.done, "Status has not completed yet"
        try:
            # TODO: this prints a traceback if cancelled, not sure why
            self._task.result()
        except Exception:
            return False
        else:
            return True

    def add_callback(self, callback: Callback):
        if self.done:
            callback(self)
        else:
            self._callbacks.append(callback)

    def _run_callbacks(self, task: Task):
        for callback in self._callbacks:
            callback(self)

    def watch(self, watcher: Callable):
        self._add_watcher(watcher)
