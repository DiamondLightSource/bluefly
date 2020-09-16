import asyncio
import os
import time
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

import h5py
import numpy as np
from event_model import compose_resource

from bluefly.core import (
    ConfigDict,
    Device,
    FilenameScheme,
    HDFResource,
    ReadableDevice,
    Status,
)


class DetectorMode(Enum):
    SOFTWARE, TRIGGERED, GATED = range(3)


class DetectorLogic:
    # TODO: should this be an ABC?
    async def open(self, file_prefix: str) -> HDFResource:
        """Open files, etc"""
        raise NotImplementedError(self)

    async def get_deadtime(self, exposure: float) -> float:
        """Get the deadtime to be added to an exposure time before next trigger"""
        raise NotImplementedError(self)

    async def arm(self, num: int, offset: int, mode: DetectorMode, exposure: float):
        """Arm for collection of num points, arranging for them to put them at offset
        into file. Exposure not used in gated mode. In software mode, acquisition
        will start immediately"""
        raise NotImplementedError(self)

    async def collect(self, num: int, callback: Callable[[int], None]):
        """Return the data collected from trigger. Iterator gives a progress
        indicator from 1..num whenever data is ready to read"""
        raise NotImplementedError(self)

    async def stop(self):
        """Stop where you are, without closing files"""
        raise NotImplementedError(self)

    async def close(self):
        """Close any files"""
        raise NotImplementedError(self)


T = TypeVar("T")


class DatumFactory:
    def __init__(self, name: str, resource: HDFResource):
        self.point_number = 0
        self._name = name
        self._resource = resource
        self._datum_cache: List[Dict] = []
        self._asset_docs_cache: List[Tuple] = []
        dirname, basename = os.path.split(resource.file_path)
        resource_d, self._datum_factory, _ = compose_resource(
            start=dict(uid="will be popped below"),
            spec=resource.spec,
            root=dirname + "/",
            resource_path=basename,
            resource_kwargs=dict(frame_per_point=1),
        )
        resource_d.pop("run_start")
        self._asset_docs_cache.append(("resource", resource_d))

    def _yield_from_cache(self, cache: List[T]) -> Generator[T, None, None]:
        items = cache.copy()
        cache.clear()
        for item in items:
            yield item

    @property
    def summary_name(self):
        return f"{self._name}_{self._resource.summary.name}"

    @property
    def data_name(self):
        return f"{self._name}_{self._resource.data[0].name}"

    def register_collections(self, indexes: Sequence[int]):
        # TODO: might want to move this to read() and collect_datums()
        with h5py.File(self._resource.file_path, "r") as f:
            values = f[self._resource.summary.dataset_path][indexes][:]
            values = np.reshape(values, values.shape[0])
        # TODO: Make this produce a single Page, rather than lots of datums
        for v in values:
            datum = self._datum_factory(
                datum_kwargs=dict(point_number=self.point_number)
            )
            self._asset_docs_cache.append(("datum", datum))
            now = time.time()
            self._datum_cache.append(
                dict(
                    # TODO: how to expose PandA multiple datasets in a single HDF file
                    data={self.data_name: datum["datum_id"], self.summary_name: v},
                    # TODO: use the timestamps from the HDF file
                    timestamps={self.data_name: now, self.summary_name: now},
                    time=now,
                    filled={self.data_name: False, self.summary_name: True},
                )
            )
            self.point_number += 1

    def collect_datums(self) -> Generator[ConfigDict, None, None]:
        yield from self._yield_from_cache(self._datum_cache)

    def collect_asset_docs(self) -> Generator[Tuple, None, None]:
        yield from self._yield_from_cache(self._asset_docs_cache)

    def describe(self) -> ConfigDict:
        with h5py.File(self._resource.file_path, "r") as f:
            data_shape = f[self._resource.data[0].dataset_path].shape
        return {
            self.data_name: dict(
                external="FILESTORE:",
                dtype="array",
                # TODO: shouldn't have to add extra dim here to be compatible with AD
                shape=(1,) + tuple(data_shape),
                source="an HDF file",
            ),
            self.summary_name: dict(
                dtype="number", shape=[], source="an HDF file", precision=0
            ),
        }

    def read(self) -> ConfigDict:
        # TODO: why are these different?
        data = self._datum_cache[-1]
        return {
            k: dict(value=v, timestamp=data["timestamps"][k])
            for k, v in data["data"].items()
        }


class DetectorDevice(ReadableDevice):
    def __init__(self, logic: DetectorLogic):
        self.logic = logic
        self._when_configured = time.time()
        self._exposure = 0.1
        self._watchers: List[Callable] = []
        self._factory: Optional[DatumFactory] = None
        self._scheme = FilenameScheme.get_instance()

    def configure(self, d: Dict[str, Any]) -> Tuple[ConfigDict, ConfigDict]:
        old_config = self.read_configuration()
        self._when_configured = time.time()
        self._exposure = d["exposure"]
        new_config = self.read_configuration()
        return old_config, new_config

    def read_configuration(self) -> ConfigDict:
        return dict(
            exposure=dict(value=self._exposure, timestamp=self._when_configured,)
        )

    def describe_configuration(self) -> ConfigDict:
        return dict(
            exposure=dict(source="user supplied parameter", dtype="number", shape=[])
        )

    def stage(self) -> List[Device]:
        self._factory = None
        return [self]

    def unstage(self) -> List[Device]:
        # TODO: would be good to return a Status object here
        asyncio.create_task(self._unstage())
        return [self]

    async def _unstage(self):
        await asyncio.gather(self.logic.close(), self._scheme.done_using_prefix())

    @property
    def hints(self):
        assert self._factory, "Not triggered yet"
        return dict(fields=[self._factory.summary_name])

    def read(self) -> ConfigDict:
        assert self._factory, "Not triggered yet"
        return self._factory.read()

    def describe(self) -> ConfigDict:
        assert self._factory, "Not triggered yet"
        return self._factory.describe()

    def collect_asset_docs(self):
        if self._factory:
            yield from self._factory.collect_asset_docs()

    def trigger(self) -> Status:
        self._trigger_task = asyncio.create_task(self._trigger())
        status = Status(self._trigger_task, self._watchers.append)
        return status

    async def _trigger(self):
        start = time.time()

        if self._factory is None:
            # beginning of the scan, open the file
            assert self.name
            file_prefix = await self._scheme.current_prefix()
            resource = await self.logic.open(file_prefix + self.name)
            self._factory = DatumFactory(self.name, resource)

        async def update_watchers():
            for _ in range(int(self._exposure / 0.1) + 1):
                for watcher in self._watchers:
                    elapsed = time.time() - start
                    watcher(
                        name=self.name,
                        current=elapsed,
                        initial=0,
                        target=self._exposure,
                        unit="s",
                        precision=3,
                        time_elapsed=elapsed,
                        fraction=elapsed / self._exposure,
                    )
                await asyncio.sleep(0.1)

        offset = self._factory.point_number
        await self.logic.arm(1, offset, DetectorMode.SOFTWARE, self._exposure)
        t = asyncio.create_task(update_watchers())
        try:

            def callback(counter):
                self._factory.register_collections([counter + offset - 1])

            await asyncio.wait_for(
                self.logic.collect(1, callback), timeout=60 + self._exposure
            )
        finally:
            t.cancel()


async def arm_detectors_triggered(
    detectors: Sequence[DetectorDevice], num: int, offset: int, period: float
):
    async def arm_detector(det: DetectorDevice):
        exposure = period - await det.logic.get_deadtime(period)
        await det.logic.arm(num, offset, DetectorMode.TRIGGERED, exposure)

    await asyncio.gather(*[arm_detector(det) for det in detectors])


async def collect_detectors(
    detectors: Sequence[DetectorDevice], num: int, callback: Callable[[str, int], None],
):
    coros = []
    for det in detectors:
        assert det.name

        def det_callback(step, name=det.name):
            callback(name, step)

        coros.append(det.logic.collect(num, det_callback))
    await asyncio.gather(*coros)


async def stop_detectors(detectors: Sequence[DetectorDevice]):
    await asyncio.gather(*[det.logic.stop() for det in detectors])
