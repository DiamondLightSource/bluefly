import asyncio
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from tempfile import mkdtemp
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

import h5py
from event_model import compose_resource

from bluefly.core import (
    ConfigDict,
    Device,
    HasSignals,
    SignalR,
    SignalRW,
    SignalX,
    Status,
)


class DetectorDriver(HasSignals):
    image_mode: SignalRW[str]  # single, multiple, continuous
    num_images: SignalRW[int]
    trigger_mode: SignalRW[str]
    start: SignalX
    stop: SignalX
    acquiring: SignalR[bool]
    array_counter: SignalRW[int]
    acquire_time: SignalRW[float]  # exposure
    acquire_period: SignalRW[float]
    array_size_x: SignalR[int]
    array_size_y: SignalR[int]


class HDFWriter(HasSignals):
    file_write_mode: SignalRW[str]  # stream, single
    swmr_mode: SignalRW[bool]
    lazy_open: SignalRW[bool]
    num_capture: SignalRW[int]
    file_path: SignalRW[str]
    file_name: SignalRW[str]
    file_template: SignalRW[str]
    pos_name_dim_n: SignalRW[str]
    start: SignalX
    stop: SignalX
    flush_now: SignalX
    array_counter: SignalRW[int]


class DetectorMode(Enum):
    SOFTWARE, TRIGGERED, GATED = range(3)


class FilenameScheme:
    file_path: Optional[str] = None
    file_template: str = "%s%s.h5"

    async def new_scan(self):
        self.file_path = mkdtemp() + os.sep


@dataclass
class FileDetails:
    file_path: str
    file_template: str
    file_name: str

    def full_path(self):
        # TODO: this need windows/linux path conversion, etc.
        return self.file_template % (self.file_path, self.file_name)


@dataclass
class DatasetDetails:
    data_shape: Tuple[int, ...]  # Fastest moving last, e.g. (768, 1024)
    data_suffix: str = "_data"
    data_path: str = "/entry/data/data"
    summary_suffix: str = "_sum"
    summary_path: str = "/entry/sum"


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

    async def get_trigger_period(self, exposure: float) -> float:
        """Get the period of time between each trigger to allow a given exposure time"""
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
        print("trigger")
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
        async for self._value in self.logic.collect(1, self._offset, self._exposure):
            print(self._offset)
            self._when_updated = time.time()
            datum = self._datum_factory(datum_kwargs=dict(point_number=self._offset))
            self._asset_docs_cache.append(("datum", datum))
            self._id = datum["datum_id"]
        self._offset += 1
        t.cancel()

    def collect_asset_docs(self):
        print("collect")
        items = self._asset_docs_cache.copy()
        self._asset_docs_cache.clear()
        for item in items:
            yield item


async def open_hdf_file(hdf: HDFWriter, file_details: FileDetails) -> asyncio.Task:
    await asyncio.gather(
        # Setup filename
        hdf.file_template.set(file_details.file_template),
        hdf.file_path.set(file_details.file_path),
        hdf.file_name.set(file_details.file_name),
        # Capture forever
        hdf.num_capture.set(0),
        # Put them in the place given by the uniqueId to support rewind
        hdf.pos_name_dim_n.set("NDUniqueID"),
        hdf.swmr_mode.set(True),
        hdf.lazy_open.set(True),
    )
    return asyncio.create_task(hdf.start())


async def hdf_flush_and_observe(
    hdf: HDFWriter, num: int, exposure: float
) -> AsyncGenerator[int, None]:
    async def flush_every_second():
        while True:
            await asyncio.sleep(1)
            await hdf.flush_now()

    flush_task = asyncio.create_task(flush_every_second())
    # Timeout if we don't get a new frame every exposure + 60s
    async for counter in hdf.array_counter.observe(timeout=exposure + 1):
        # TODO: we might skip some uids, should fill them in here
        if counter != 0:
            # uid starts from 0, counter from 1
            yield counter - 1
        if counter == num:
            # This won't pick up last frame dropped, but that's probably ok
            break
    flush_task.cancel()
    # One last flush to make sure everything has cleared
    await hdf.flush_now()


async def setup_n_frames(
    driver: DetectorDriver, hdf: HDFWriter, num: int, offset: int, exposure: float
):
    if num == 1:
        image_mode = "Single"
    else:
        image_mode = "Multiple"
    await asyncio.gather(
        driver.image_mode.set(image_mode),
        driver.num_images.set(num),
        driver.acquire_time.set(exposure),
        driver.array_counter.set(offset - 1),
        # Zero the array counter so we can see when we get frames
        hdf.array_counter.set(0),
    )


def trigger_period(
    exposure: float, readout_time: float, frequency_accuracy: float
) -> float:
    """Given a fixed exposure time, and a crystal frequency accuracy, what should the
    time between trigger rising edges be"""
    period = exposure + readout_time
    period += frequency_accuracy * period / 1000000
    return period


@dataclass
class AndorLogic(DetectorLogic):
    driver: DetectorDriver
    hdf: HDFWriter
    _hdf_start_task: asyncio.Task = field(init=False)
    _hdf_path: str = field(init=False)

    async def open(self, file_details: FileDetails) -> DatasetDetails:
        self._hdf_start_task = await open_hdf_file(self.hdf, file_details)
        self._hdf_path = file_details.full_path()
        return DatasetDetails(
            data_shape=await asyncio.gather(
                self.driver.array_size_y.get(), self.driver.array_size_x.get(),
            ),
        )

    async def trigger(self, num: int, offset: int, mode: DetectorMode, exposure: float):
        # Choose the right Andor specific trigger mode
        await self.driver.trigger_mode.set(
            {
                DetectorMode.SOFTWARE: "Software",
                DetectorMode.TRIGGERED: "External",
                DetectorMode.GATED: "Gate",
            }[mode]
        )
        # Setup driver and HDF writer for n frames
        await setup_n_frames(self.driver, self.hdf, num, offset, exposure)
        # Need to overwrite here for this driver in particular
        await self.driver.acquire_period.set(await self.get_trigger_period(exposure))
        # Kick off the driver to take data
        asyncio.create_task(self.driver.start())

    async def collect(
        self, num: int, offset: int, exposure: float
    ) -> AsyncGenerator[float, None]:
        # monitor progress
        async for i in hdf_flush_and_observe(self.hdf, num, exposure):
            yield float(h5py.File(self._hdf_path, "r")["/entry/sum"][i + offset][0][0])

    async def get_trigger_period(self, exposure: float) -> float:
        # Might need to prod the driver to do these calcs
        return trigger_period(exposure, readout_time=0.02, frequency_accuracy=50)

    async def stop(self):
        await self.driver.stop()

    async def close(self):
        await self.hdf.stop()
        await self._hdf_start_task
