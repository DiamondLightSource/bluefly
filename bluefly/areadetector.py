import asyncio
from dataclasses import dataclass, field
from typing import AsyncGenerator

import h5py

from bluefly.core import HasSignals, SignalR, SignalRW, SignalX
from bluefly.detector import DatasetDetails, DetectorLogic, DetectorMode, FileDetails


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
    hdf: HDFWriter, num: int, timeout: float
) -> AsyncGenerator[int, None]:
    async def flush_every_second():
        while True:
            await asyncio.sleep(1)
            await hdf.flush_now()

    flush_task = asyncio.create_task(flush_every_second())
    try:
        async for counter in hdf.array_counter.observe(timeout):
            # TODO: we might skip some uids, should fill them in here
            if counter != 0:
                # uid starts from 0, counter from 1
                yield counter - 1
            if counter == num:
                # This won't pick up last frame dropped, but that's probably ok
                break
    finally:
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
        period = exposure + await self.get_deadtime(exposure)
        await self.driver.acquire_period.set(period)
        # Kick off the driver to take data
        asyncio.create_task(self.driver.start())

    async def collect(
        self, num: int, offset: int, timeout: float
    ) -> AsyncGenerator[float, None]:
        # monitor progress
        async for i in hdf_flush_and_observe(self.hdf, num, timeout):
            dset = h5py.File(self._hdf_path, "r")["/entry/sum"]
            while dset.shape[0] <= i + offset:
                await asyncio.sleep(0.1)
                dset.id.refresh()
            yield float(dset[i + offset][0][0])

    async def get_deadtime(self, exposure: float) -> float:
        # Might need to prod the driver to do these calcs
        return calc_deadtime(exposure, readout_time=0.02, frequency_accuracy=50)

    async def stop(self):
        await self.driver.stop()

    async def close(self):
        await self.hdf.stop()
        await self._hdf_start_task
