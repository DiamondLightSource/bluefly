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
from bluefly.core import NamedDevices, SignalCollector, TmpFilenameScheme
from bluefly.simprovider import SimProvider


@pytest.mark.asyncio
async def test_fly_scan():
    # need to do this so resume can get it
    set_bluesky_event_loop(asyncio.get_running_loop())
    async with SignalCollector(), NamedDevices(), TmpFilenameScheme():
        sim = SignalCollector.add_provider(sim=SimProvider(), set_default=True)
        pmac1 = pmac.PMAC("BLxxI-MO-PMAC-01:")
        t1x = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:X"))
        t1y = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Y"))
        t1z = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Z"))
        det_logic = areadetector.AndorLogic(
            areadetector.DetectorDriver("BLxxI-EA-DET-01:DRV"),
            areadetector.HDFWriter("BLxxI-EA-DET-01:HDF5"),
        )
        det = detector.DetectorDevice(det_logic)
        scan = fly.FlyDevice([det], fly.PMACMasterFlyLogic(pmac1, [t1x, t1y, t1z]))
    assert t1x.name == "t1x"
    assert scan.name == "scan"
    # Fill in the trajectory logic
    areadetector_sim.sim_detector_logic(sim, det_logic.driver, det_logic.hdf, t1x, t1y)
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
        watcher = Mock()
        done = Mock()
        await scan.kickoff()
        s = scan.complete()
        s.watch(watcher)
        s.add_callback(done)
        assert not s.done
        await asyncio.sleep(1.25)
        watcher.assert_called_once_with(
            name="scan",
            current=1,
            initial=0,
            target=6,
            unit="",
            precision=0,
            time_elapsed=pytest.approx(1.0, abs=0.2),
        )
        assert await t1x.motor.readback.get() == 2.0
        scan.pause()
        watcher.reset_mock()
        assert not s.done
        await asyncio.sleep(0.75)
        assert s.done
        assert not s.success
        watcher.assert_not_called()
        scan.resume()
        assert not s.done
        done.assert_not_called()
        await asyncio.sleep(2.75)
        assert s.done
        assert s.success
        assert await t1x.motor.readback.get() == 2.0
        assert watcher.call_count == 3
        assert [c[1]["current"] for c in watcher.call_args_list] == [2, 4, 6]
        assert [c[1]["time_elapsed"] for c in watcher.call_args_list] == pytest.approx(
            [3, 4, 4.5], abs=0.3
        )
        done.assert_called_once_with(s)
        done.reset_mock()
        s.add_callback(done)
        done.assert_called_once_with(s)


@pytest.mark.asyncio
async def test_motor_moving():
    async with SignalCollector(), NamedDevices():
        sim = SignalCollector.add_provider(sim=SimProvider(), set_default=True)
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
    async with SignalCollector(), TmpFilenameScheme(), NamedDevices():
        sim = SignalCollector.add_provider(sim=SimProvider(), set_default=True)
        det = detector.DetectorDevice(
            areadetector.AndorLogic(
                areadetector.DetectorDriver("BLxxI-EA-DET-01:DRV"),
                areadetector.HDFWriter("BLxxI-EA-DET-01:HDF5"),
            )
        )
        x = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:X"))
        y = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Y"))
    motor_sim.sim_motor_logic(sim, x)
    motor_sim.sim_motor_logic(sim, y)
    areadetector_sim.sim_detector_logic(sim, det.logic.driver, det.logic.hdf, x, y)

    assert det.stage() == [det]
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
                "root": await det._scheme.current_prefix(),
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
    assert det.unstage() == [det]
    # Wait for HDF file to be closed, can't actually check this, but it stops
    # "Task was destroyed but it is pending!" messages
    await asyncio.sleep(0.5)
