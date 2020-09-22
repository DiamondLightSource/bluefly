import asyncio

import h5py
import numpy as np

from bluefly.areadetector import DetectorDriver, HDFWriter
from bluefly.motor import MotorDevice
from bluefly.simprovider import SimProvider


def make_gaussian_blob(width: int, height: int) -> np.ndarray:
    """Make a Gaussian Blob with float values in range 0..1"""
    x, y = np.meshgrid(np.linspace(-1, 1, width), np.linspace(-1, 1, height))
    d = np.sqrt(x * x + y * y)
    blob = np.exp(-(d ** 2))
    return blob


def interesting_pattern(x: float, y: float) -> float:
    """This function is interesting in x and y in range -10..10, returning
    a float value in range 0..1
    """
    z = 0.5 + (np.sin(x) ** 10 + np.cos(10 + y * x) * np.cos(x)) / 2
    return z


DATA_PATH = "/entry/data/data"
UID_PATH = "/entry/uid"
SUM_PATH = "/entry/sum"


def sim_detector_logic(
    p: SimProvider,
    driver: DetectorDriver,
    hdf: HDFWriter,
    x: MotorDevice,
    y: MotorDevice,
    width: int = 320,
    height: int = 240,
):
    stopping = asyncio.Event()
    # The detector image we will modify for each image (0..255 range)
    blob = make_gaussian_blob(width, height) * 255
    hdf_file = None
    p.set_value(driver.array_size_x, width)
    p.set_value(driver.array_size_y, height)

    @p.on_call(hdf.start)
    async def do_hdf_start():
        file_path = p.get_value(hdf.file_template) % (
            p.get_value(hdf.file_path),
            p.get_value(hdf.file_name),
        )
        nonlocal hdf_file
        hdf_file = h5py.File(file_path, "w", libver="latest")
        # Data written in a big stack, growing in that dimension
        hdf_file.create_dataset(
            DATA_PATH,
            dtype=np.uint8,
            shape=(1, height, width),
            maxshape=(None, height, width),
        )
        for path, dtype in {UID_PATH: np.int32, SUM_PATH: np.float64}.items():
            # Areadetector attribute datasets have the same dimesionality as the data
            hdf_file.create_dataset(
                path, dtype=dtype, shape=(1, 1, 1), maxshape=(None, 1, 1), fillvalue=-1
            )
        hdf_file.swmr_mode = True

    @p.on_call(driver.start)
    async def do_driver_start():
        stopping.clear()
        # areaDetector drivers start from array_counter + 1
        offset = p.get_value(driver.array_counter) + 1
        exposure = p.get_value(driver.acquire_time)
        period = p.get_value(driver.acquire_period)
        for i in range(p.get_value(driver.num_images)):
            try:
                # See if we got told to stop
                await asyncio.wait_for(stopping.wait(), period)
            except asyncio.TimeoutError:
                # Carry on
                pass
            else:
                # Stop now
                break
            uid = i + offset
            # Resize the datasets so they fit
            for path in (DATA_PATH, SUM_PATH, UID_PATH):
                ds = hdf_file[path]
                expand_to = tuple(max(*z) for z in zip((uid + 1, 1, 1), ds.shape))
                ds.resize(expand_to)
            intensity = interesting_pattern(
                p.get_value(x.motor.readback), p.get_value(y.motor.readback)
            )
            detector_data = (blob * intensity * exposure / period).astype(np.uint8)
            hdf_file[DATA_PATH][uid] = detector_data
            hdf_file[UID_PATH][uid] = uid
            hdf_file[SUM_PATH][uid] = np.sum(detector_data)
            p.set_value(hdf.array_counter, p.get_value(hdf.array_counter) + 1)

    @p.on_call(hdf.flush_now)
    async def do_hdf_flush():
        # Note that UID comes last so anyone monitoring knows the data is there
        for path in (DATA_PATH, SUM_PATH, UID_PATH):
            hdf_file[path].flush()

    @p.on_call(hdf.stop)
    async def do_hdf_close():
        hdf_file.close()

    @p.on_call(driver.stop)
    async def do_driver_stop():
        stopping.set()
