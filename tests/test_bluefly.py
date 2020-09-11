import asyncio
import os
import time
from unittest.mock import ANY, Mock, call, patch

import h5py
import numpy as np
import pytest
from bluesky.run_engine import set_bluesky_event_loop
from scanpointgenerator import CompoundGenerator, LineGenerator

from bluefly import (
    areadetector,
    areadetector_sim,
    detector,
    fly,
    motor,
    motor_sim,
    pmac,
    pmac_sim,
)
from bluefly.core import SignalCollector
from bluefly.simprovider import SimProvider


@pytest.mark.asyncio
async def test_fly_scan():
    # need to do this so SimProvider can get it for making queues
    set_bluesky_event_loop(asyncio.get_running_loop())
    async with SignalCollector() as sc:
        sim = sc.add_provider(sim=SimProvider(), set_default=True)
        pmac1 = pmac.PMAC("BLxxI-MO-PMAC-01:")
        t1x = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:X"))
        t1y = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Y"))
        t1z = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Z"))
        scheme = detector.FilenameScheme()
        det = detector.DetectorDevice(
            areadetector.AndorLogic(
                areadetector.DetectorDriver("BLxxI-EA-DET-01:DRV"),
                areadetector.HDFWriter("BLxxI-EA-DET-01:HDF5"),
            ),
            scheme,
        )
        scan = fly.FlyDevice(
            fly.PMACMasterFlyLogic(pmac1, [det], [t1x, t1y, t1z]), scheme
        )
    assert t1x.name == "t1x"
    assert scan.name == "scan"
    # Fill in the trajectory logic
    areadetector_sim.sim_detector_logic(sim, det.logic.driver, det.logic.hdf, t1x, t1y)
    pmac_sim.sim_trajectory_logic(sim, pmac1.traj, a=t1x, b=t1y)
    # Configure a scan
    generator = CompoundGenerator(
        generators=[
            LineGenerator("t1y", "mm", 0, 1, 2),
            LineGenerator("t1x", "mm", 1, 2, 3),
        ],
        duration=0.5,
    )
    scan.configure(dict(generator=generator))
    with patch("bluefly.pmac.BATCH_SIZE", 4):
        m = Mock()
        s = scan.trigger()
        s.watch(m)
        assert not s.done
        await asyncio.sleep(0.75)
        m.assert_called_once_with(
            name="scan",
            current=1,
            initial=0,
            target=6,
            unit="",
            precision=0,
            time_elapsed=pytest.approx(0.5, abs=0.2),
            fraction=1 / 6,
        )
        assert await t1x.motor.readback.get() == 1.5
        scan.pause()
        m.reset_mock()
        assert not s.done
        await asyncio.sleep(0.75)
        assert s.done
        # TODO: this raises CancelledError at the moment
        # assert not s.success
        m.assert_not_called()
        scan.resume()
        s = scan.trigger()
        m = Mock()
        done = Mock()
        s.watch(m)
        s.add_callback(done)
        assert not s.done
        done.assert_not_called()
        await asyncio.sleep(2.75)
        assert s.done
        assert s.success
        assert await t1x.motor.readback.get() == 2.0
        assert m.call_count == 5  # 2..6
        assert m.call_args_list[-1][1]["current"] == 6
        assert m.call_args_list[-1][1]["time_elapsed"] == pytest.approx(
            0.75 + 0.75 + 5 * 0.5, abs=0.3
        )
        done.assert_called_once_with(s)
        done.reset_mock()
        s.add_callback(done)
        done.assert_called_once_with(s)


@pytest.mark.asyncio
async def test_motor_moving():
    # need to do this so SimProvider can get it for making queues
    set_bluesky_event_loop(asyncio.get_running_loop())
    async with SignalCollector() as sc:
        sim = sc.add_provider(sim=SimProvider(), set_default=True)
        x = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:X"))
    motor_sim.sim_motor_logic(sim, x)

    s = x.set(0.55)
    m = Mock()
    done = Mock()
    s.add_callback(done)
    s.watch(m)
    assert not s.done
    done.assert_not_called()
    await asyncio.sleep(0.3)
    assert not s.done
    assert m.call_count == 3
    await asyncio.sleep(0.3)
    assert s.done
    assert m.call_count == 6
    assert m.call_args_list[1] == call(
        name="x",
        current=0.1,
        initial=0.0,
        target=0.55,
        unit="mm",
        precision=3,
        time_elapsed=pytest.approx(0.1, abs=0.05),
        fraction=1 / 5.5,
    )
    await x.trigger()
    assert x.read()["x"]["value"] == 0.55
    assert x.describe()["x"]["source"] == "BLxxI-MO-TABLE-01:X.readback"
    assert x.read_configuration() == {}
    assert x.describe_configuration() == {}
    s = x.set(1.5)
    s.add_callback(done)
    await asyncio.sleep(0.2)
    x.stop()
    await asyncio.sleep(0.2)
    assert s.done
    with pytest.raises(RuntimeError) as cm:
        await s
    assert str(cm.value) == "Motor was stopped"


@pytest.mark.asyncio
async def test_areadetector_step_scan():
    # need to do this so SimProvider can get it for making queues
    set_bluesky_event_loop(asyncio.get_running_loop())
    async with SignalCollector() as sc:
        sim = sc.add_provider(sim=SimProvider(), set_default=True)
        scheme = detector.FilenameScheme()
        det = detector.DetectorDevice(
            areadetector.AndorLogic(
                areadetector.DetectorDriver("BLxxI-EA-DET-01:DRV"),
                areadetector.HDFWriter("BLxxI-EA-DET-01:HDF5"),
            ),
            scheme,
        )
        x = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:X"))
        y = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Y"))
    motor_sim.sim_motor_logic(sim, x)
    motor_sim.sim_motor_logic(sim, y)
    areadetector_sim.sim_detector_logic(sim, det.logic.driver, det.logic.hdf, x, y)

    assert det.stage() == [det]
    assert scheme.file_path is None
    det.configure(dict(exposure=1.0))
    now = time.time()
    await det.trigger()
    assert time.time() - now == pytest.approx(1.0, abs=0.1)
    assert det.read()["det_sum"]["value"] == 819984.0
    docs = list(det.collect_asset_docs())
    assert docs == [
        (
            "resource",
            {
                "path_semantics": "posix",
                "resource_kwargs": {"frame_per_point": 1},
                "resource_path": "det.h5",
                "root": scheme.file_path,
                "spec": "AD_HDF5",
                "uid": ANY,
            },
        ),
        (
            "datum",
            {"datum_id": ANY, "datum_kwargs": {"point_number": 0}, "resource": ANY},
        ),
    ]
    assert docs[1][1]["resource"] == docs[0][1]["uid"]
    assert docs[1][1]["datum_id"] == docs[0][1]["uid"] + "/0"
    fname = os.path.join(docs[0][1]["root"], docs[0][1]["resource_path"])
    f = h5py.File(fname, "r")
    assert f["/entry/sum"].shape == (1, 1, 1)
    assert f["/entry/data/data"].shape == (1, 240, 320)
    assert f["/entry/sum"][0][0][0] == 819984.0
    assert np.sum(f["/entry/data/data"][0]) == 819984.0
    await x.set(3)
    await det.trigger()
    assert det.read()["det_sum"]["value"] == 9726824.0
    docs2 = list(det.collect_asset_docs())
    assert docs2 == [
        (
            "datum",
            {"datum_id": ANY, "datum_kwargs": {"point_number": 1}, "resource": ANY},
        )
    ]
    assert docs2[0][1]["datum_id"] == docs[0][1]["uid"] + "/1"
    assert f["/entry/sum"].shape == (2, 1, 1)
    assert f["/entry/data/data"].shape == (2, 240, 320)
    assert f["/entry/sum"][1][0][0] == 9726824.0
    assert np.sum(f["/entry/data/data"][1]) == 9726824.0
