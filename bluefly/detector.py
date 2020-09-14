import asyncio
import os
import time
from enum import Enum
from tempfile import mkdtemp
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from event_model import compose_resource

from bluefly.core import (
    ConfigDict,
    DatasetDetails,
    Device,
    FileDetails,
    ReadableDevice,
    Status,
)


class FilenameScheme:
    file_path: Optional[str] = None
    file_template: str = "%s%s.h5"

    async def new_scan(self):
        self.file_path = mkdtemp() + os.sep


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
    ) -> AsyncGenerator[Sequence[float], None]:
        """Return the data collected from trigger. Iterator gives a sequence of
        "summary values" for each batch of frames."""
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


T = TypeVar("T")


class DatumFactory:
    def __init__(self, name: str, details: FileDetails, datasets: DatasetDetails):
        self.point_number = 0
        self._name = name
        self._details = details
        self._datasets = datasets
        self._datum_cache: List[Dict] = []
        self._asset_docs_cache: List[Tuple] = []
        resource, self._datum_factory, _ = compose_resource(
            start=dict(uid="will be popped below"),
            spec="AD_HDF5",
            root=self._details.file_path,
            resource_path=self._details.full_path()[len(self._details.file_path) :],
            resource_kwargs=dict(frame_per_point=1),
        )
        resource.pop("run_start")
        self._asset_docs_cache.append(("resource", resource))

    def _yield_from_cache(self, cache: List[T]) -> Generator[T, None, None]:
        items = cache.copy()
        cache.clear()
        for item in items:
            yield item

    @property
    def data_name(self):
        return self._name + self._datasets.data_suffix

    @property
    def summary_name(self):
        return self._name + self._datasets.summary_suffix

    def register_collections(self, values: Sequence[float]):
        # TODO: Make this produce a single Page, rather than lots of datums
        for v in values:
            datum = self._datum_factory(
                datum_kwargs=dict(point_number=self.point_number)
            )
            self._asset_docs_cache.append(("datum", datum))
            now = time.time()
            self._datum_cache.append(
                dict(
                    data={self.data_name: datum["datum_id"], self.summary_name: v},
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
        return {
            self.data_name: dict(
                external="FILESTORE:",
                dtype="array",
                # TODO: shouldn't have to add extra dim here to be compatible with AD
                shape=(1,) + tuple(self._datasets.data_shape),
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
    def __init__(self, logic: DetectorLogic, scheme: FilenameScheme):
        self.logic = logic
        self._when_configured = time.time()
        self._exposure = 0.1
        self._watchers: List[Callable] = []
        self._factory: Optional[DatumFactory] = None
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

    def stage(self) -> List[Device]:
        self._factory = None
        return [self]

    def unstage(self) -> List[Device]:
        # TODO: would be good to return a Status object here
        asyncio.create_task(self.logic.close())
        return [self]

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
            await self._scheme.new_scan()
            details = FileDetails(
                self._scheme.file_path, self._scheme.file_template, self.name
            )
            datasets = await self.logic.open(details)
            self._factory = DatumFactory(self.name, details, datasets)

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

        await self.logic.trigger(
            1, self._factory.point_number, DetectorMode.SOFTWARE, self._exposure
        )
        t = asyncio.create_task(update_watchers())
        async for value in self.logic.collect(
            1, self._factory.point_number, timeout=self._exposure + 60
        ):
            self._factory.register_collections(value)
        t.cancel()


async def open_detectors(
    detector_details: Dict[DetectorDevice, FileDetails]
) -> Dict[DetectorDevice, DatasetDetails]:
    coros = [det.logic.open(details) for det, details in detector_details.items()]
    datasets = await asyncio.gather(*coros)
    datasets_dict = dict(zip(detector_details, datasets))
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
    queue: "asyncio.Queue[Tuple[str, Sequence[float]]]",
    timeout: float,
):
    async def collect_and_report_step(det: DetectorDevice):
        assert det.name
        async for value in det.logic.collect(num, offset, timeout):
            queue.put_nowait((det.name, value))

    coros = [collect_and_report_step(det) for det in detectors]
    await asyncio.gather(*coros)


async def stop_detectors(detectors: Sequence[DetectorDevice]):
    await asyncio.gather(*[det.logic.stop() for det in detectors])


async def close_detectors(detectors: Sequence[DetectorDevice]):
    await asyncio.gather(*[det.logic.close() for det in detectors])
