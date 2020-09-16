import asyncio
import os
from dataclasses import dataclass, field
from typing import Callable, List

from bluefly.core import (
    HasSignals,
    HDFDatasetResource,
    HDFResource,
    SignalR,
    SignalRW,
    SignalX,
)
from bluefly.detector import DetectorLogic, DetectorMode


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


async def open_hdf_file(hdf: HDFWriter, resource: HDFResource) -> asyncio.Task:
    # TODO: translate linux/windows paths
    dirname, basename = os.path.split(resource.file_path)
    await asyncio.gather(
        # Setup filename
        hdf.file_template.set("%s%s"),
        hdf.file_path.set(dirname + "/"),
        hdf.file_name.set(basename),
        # Capture forever
        hdf.num_capture.set(0),
        # Put them in the place given by the uniqueId to support rewind
        hdf.pos_name_dim_n.set("NDUniqueID"),
        hdf.swmr_mode.set(True),
        hdf.lazy_open.set(True),
    )
    return asyncio.create_task(hdf.start())


async def hdf_flush_and_observe(
    hdf: HDFWriter, num: int, callback: Callable[[int], None]
):
    counters: List[int] = []

    async def flush_and_callback():
        await hdf.flush_now()
        if counters:
            callback(counters[-1])
            counters.clear()

    async def period_flush_and_callback():
        while True:
            # Every second flush and callback progress if changed
            await asyncio.gather(asyncio.sleep(1), flush_and_callback())

    flush_task = asyncio.create_task(period_flush_and_callback())
    try:
        async for counter in hdf.array_counter.observe():
            # TODO: handle dropped frame counter too
            if counter > 0:
                counters.append(counter)
            if counter == num:
                break
    finally:
        flush_task.cancel()
    # One last flush and callback to make sure everything has cleared
    await flush_and_callback()


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


def calc_deadtime(
    exposure: float, readout_time: float, frequency_accuracy: float
) -> float:
    """Given a fixed exposure time, and a crystal frequency accuracy, what should the
    time between trigger rising edges be"""
    period = exposure + readout_time
    period += frequency_accuracy * period / 1000000
    return period - exposure


@dataclass
class AndorLogic(DetectorLogic):
    driver: DetectorDriver
    hdf: HDFWriter
    _hdf_start_task: asyncio.Task = field(init=False)

    async def open(self, file_prefix: str) -> HDFResource:
        resource = HDFResource(
            data=[HDFDatasetResource()],
            summary=HDFDatasetResource("sum", "/entry/sum"),
            file_path=file_prefix + ".h5",
        )
        self._hdf_start_task = await open_hdf_file(self.hdf, resource)
        return resource

    async def get_deadtime(self, exposure: float) -> float:
        # Might need to prod the driver to do these calcs
        return calc_deadtime(exposure, readout_time=0.02, frequency_accuracy=50)

    async def arm(self, num: int, offset: int, mode: DetectorMode, exposure: float):
        # Choose the right Andor specific trigger mode (made up examples)
        await self.driver.trigger_mode.set(
            {
                DetectorMode.SOFTWARE: "Immediate",
                DetectorMode.TRIGGERED: "External",
                DetectorMode.GATED: "Gated",
            }[mode]
        )
        # Setup driver and HDF writer for n frames
        await setup_n_frames(self.driver, self.hdf, num, offset, exposure)
        # Need to overwrite here for this driver in particular
        period = exposure + await self.get_deadtime(exposure)
        await self.driver.acquire_period.set(period)
        # Kick off the driver to take data
        asyncio.create_task(self.driver.start())

    async def collect(self, num: int, callback: Callable[[int], None]):
        # Monitor progress, calling back on each flush
        await hdf_flush_and_observe(self.hdf, num, callback)

    async def stop(self):
        await self.driver.stop()

    async def close(self):
        await self.hdf.stop()
        await self._hdf_start_task
