import asyncio
from unittest.mock import Mock, call, patch

import pytest
from bluesky.run_engine import set_bluesky_event_loop
from scanpointgenerator import CompoundGenerator, LineGenerator

from bluefly.core import SignalCollector
from bluefly.motor import SettableMotor, sim_motor_logic
from bluefly.myscan import MyFlyScanLogic
from bluefly.pmac import PMAC, PMACRawMotor, sim_trajectory_logic
from bluefly.scan import FlyScanDevice
from bluefly.simprovider import SimProvider


@pytest.mark.asyncio
async def test_my_scan():
    # need to do this so SimProvider can get it for making queues
    set_bluesky_event_loop(asyncio.get_running_loop())
    async with SignalCollector() as sc:
        sim = sc.add_provider(sim=SimProvider(), set_default=True)
        pmac1 = PMAC("BLxxI-MO-PMAC-01:")
        t1x = SettableMotor(PMACRawMotor("BLxxI-MO-TABLE-01:X"))
        t1y = SettableMotor(PMACRawMotor("BLxxI-MO-TABLE-01:Y"))
        t1z = SettableMotor(PMACRawMotor("BLxxI-MO-TABLE-01:Z"))
        scan = FlyScanDevice(MyFlyScanLogic(pmac1, [t1x, t1y, t1z]))
    assert pmac1.name == "pmac1"
    assert t1x.name == "t1x"
    assert scan.name == "scan"
    # Fill in the trajectory logic
    sim_trajectory_logic(sim, pmac1.traj)
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
        assert m.call_count == 2  # 0..1
        assert m.call_args_list[-1] == call(
            name="scan",
            current=1,
            initial=0,
            target=6,
            unit="",
            precision=0,
            time_elapsed=pytest.approx(0.5, abs=0.2),
            fraction=1 / 6,
        )
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
        assert m.call_count == 6  # 1..6
        assert m.call_args_list[-1][1]["current"] == 6
        assert m.call_args_list[-1][1]["time_elapsed"] == pytest.approx(
            0.75 + 0.75 + 5 * 0.5, abs=0.3
        )
        assert s.done
        assert s.success
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
        x = SettableMotor(PMACRawMotor("BLxxI-MO-TABLE-01:X"))
    sim_motor_logic(sim, x.motor)

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
