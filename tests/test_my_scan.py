import asyncio
from unittest.mock import Mock, call, patch

import pytest
from bluesky.run_engine import set_bluesky_event_loop
from scanpointgenerator import CompoundGenerator, LineGenerator

from bluefly import pmac
from bluefly.core import SignalCollector
from bluefly.myscan import MyFlyScanLogic
from bluefly.scan import FlyScanDevice
from bluefly.simprovider import SimProvider


@pytest.mark.asyncio
async def test_my_scan():
    # need to do this so SimProvider can get it for making queues
    set_bluesky_event_loop(asyncio.get_running_loop())
    async with SignalCollector() as sc:
        sim = sc.add_provider(sim=SimProvider(), set_default=True)
        pmac1 = pmac.PMAC("PMAC-01:")
        t1x = pmac.PMACRawMotor("TABLE-01:X")
        t1y = pmac.PMACRawMotor("TABLE-01:Y")
        t1z = pmac.PMACRawMotor("TABLE-01:Z")
        scan = FlyScanDevice(MyFlyScanLogic(pmac1, [t1x, t1y, t1z]))
    assert pmac1.name == "pmac1"
    assert t1x.name == "t1x"
    assert scan.name == "scan"
    # Fill in the trajectory logic
    pmac.sim_trajectory_logic(sim, pmac1.traj)
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
