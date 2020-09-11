import asyncio
import time
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Sequence, Tuple

from event_model import compose_resource

from bluefly.core import (
    ConfigDict,
    DatasetDetails,
    Device,
    FileDetails,
    FilenameScheme,
    Status,
)


class DetectorMode(Enum):
    SOFTWARE, TRIGGERED, GATED = range(3)


class DetectorLogic:
    async def open(self, file_details: FileDetails) -> DatasetDetails:
        """Open files, etc"""
        raise NotImplementedError(self)

    async def trigger(self, num: int, offset: int, mode: DetectorMode, exposure: float):
        """Trigger collection of num points, arranging for them to put them at offset
        into file. Exposure not used in gated mode"""
        raise NotImplementedError(self)

    async def collect(
        self, num: int, offset: int, exposure: float
    ) -> AsyncGenerator[float, None]:
        """Return the data collected from trigger. Iterator gives a "summary value"
        for each frame."""
        raise NotImplementedError(self)
        yield 0

    async def get_deadtime(self, exposure: float) -> float:
        """Get the deadtime to be added to an exposure time before next trigger"""
        raise NotImplementedError(self)

    async def stop(self):
        """Stop where you are, without closing files"""
        raise NotImplementedError(self)

    async def close(self):
        """Close any files"""
        raise NotImplementedError(self)


class DetectorDevice(Device):
    def __init__(self, logic: DetectorLogic, scheme: FilenameScheme):
        self.logic = logic
        self._when_configured = time.time()
        self._when_updated = time.time()
        self._exposure = 0.1
        self._watchers: List[Callable] = []
        # Summary value
        self._value = 0.0
        # Data id
        self._id = ""
        self._datasets: Optional[DatasetDetails] = None
        self._asset_docs_cache: List[Tuple] = []
        self._datum_factory = None
        # TODO: should we pass using a context manager?
        self._scheme = scheme

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

    def _data_name(self):
        assert self.name, self
        assert self._datasets, self
        return self.name + self._datasets.data_suffix

    def _summary_name(self):
        assert self.name, self
        assert self._datasets, self
        return self.name + self._datasets.summary_suffix

    def stage(self) -> List[Device]:
        self._offset = 0
        return [self]

    def read(self) -> ConfigDict:
        return {
            self._data_name(): dict(value=self._id, timestamp=self._when_updated),
            self._summary_name(): dict(value=self._value, timestamp=self._when_updated),
        }

    @property
    def hints(self):
        return dict(fields=[self._summary_name()])

    def describe(self) -> ConfigDict:
        assert self._datasets, self
        return {
            self._data_name(): dict(
                external="FILESTORE:",
                dtype="array",
                # TODO: shouldn't have to add extra dim here to be compatible with AD
                shape=(1,) + tuple(self._datasets.data_shape),
                source="an HDF file",
            ),
            self._summary_name(): dict(
                dtype="number", shape=[], source="an HDF file", precision=0
            ),
        }

    def unstage(self) -> List[Device]:
        # TODO: would be good to return a Status object here
        asyncio.create_task(self.logic.close())
        return [self]

    def trigger(self) -> Status:
        self._trigger_task = asyncio.create_task(self._trigger())
        status = Status(self._trigger_task, self._watchers.append)
        return status

    async def _trigger(self):
        start = time.time()

        if self._offset == 0:
            # beginning of the scan, open the file
            await self._scheme.new_scan()
            assert self.name
            details = FileDetails(
                self._scheme.file_path, self._scheme.file_template, self.name
            )
            datasets = await self.logic.open(details)
            resource, self._datum_factory, _ = compose_resource(
                start=dict(uid="will be popped below"),
                spec="AD_HDF5",
                root=details.file_path,
                resource_path=details.full_path()[len(details.file_path) :],
                resource_kwargs=dict(frame_per_point=1),
            )
            resource.pop("run_start")
            self._asset_docs_cache.append(("resource", resource))
            self._datasets = datasets

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

        await self.logic.trigger(1, self._offset, DetectorMode.SOFTWARE, self._exposure)
        t = asyncio.create_task(update_watchers())
        async for self._value in self.logic.collect(
            1, self._offset, timeout=self._exposure + 60
        ):
            self._when_updated = time.time()
            datum = self._datum_factory(datum_kwargs=dict(point_number=self._offset))
            self._asset_docs_cache.append(("datum", datum))
            self._id = datum["datum_id"]
        self._offset += 1
        t.cancel()

    def collect_asset_docs(self):
        items = self._asset_docs_cache.copy()
        self._asset_docs_cache.clear()
        for item in items:
            yield item


async def open_detectors(
    detectors: Sequence[DetectorDevice], file_details: FileDetails
) -> Dict[str, DatasetDetails]:
    coros = [det.logic.open(file_details) for det in detectors]
    datasets = await asyncio.gather(*coros)
    datasets_dict = {}
    for det, ds in zip(detectors, datasets):
        assert det.name
        datasets_dict[det.name] = ds
    return datasets_dict


async def arm_detectors_triggered(
    detectors: Sequence[DetectorDevice], num: int, offset: int, period: float
):
    async def arm_detector(det: DetectorDevice):
        exposure = period - await det.logic.get_deadtime(period)
        await det.logic.trigger(num, offset, DetectorMode.TRIGGERED, exposure)

    await asyncio.gather(*[arm_detector(det) for det in detectors])


async def collect_detectors(
    detectors: Sequence[DetectorDevice],
    num: int,
    offset: int,
    queue: "asyncio.Queue[int]",
    timeout: float,
):
    steps = {det: offset for det in detectors}

    async def collect_and_report_step(det: DetectorDevice):
        async for _ in det.logic.collect(num, offset, timeout):
            old_step = min(steps.values())
            steps[det] += 1
            new_step = min(steps.values())
            if new_step > old_step:
                queue.put_nowait(new_step)

    coros = [collect_and_report_step(det) for det in detectors]
    await asyncio.gather(*coros)


async def stop_detectors(detectors: Sequence[DetectorDevice]):
    await asyncio.gather(*[det.logic.stop() for det in detectors])


async def close_detectors(detectors: Sequence[DetectorDevice]):
    await asyncio.gather(*[det.logic.close() for det in detectors])
