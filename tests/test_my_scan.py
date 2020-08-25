import asyncio
from unittest.mock import Mock, patch

import pytest
from bluesky.run_engine import set_bluesky_event_loop
from scanpointgenerator import CompoundGenerator, LineGenerator

from bluefly.myscan import MyFlyScanLogic
from bluefly.pmac.sim import SimPMAC
from bluefly.scan import FlyScanDevice


@pytest.mark.asyncio
async def test_my_scan():
    # need to do this so SimProvider can get it for making queues
    set_bluesky_event_loop(asyncio.get_running_loop())
    sim_pmac = SimPMAC()
    logic = MyFlyScanLogic(sim_pmac.motors, sim_pmac.cs_list, sim_pmac.traj)
    scan = FlyScanDevice("scan", logic)
    generator = CompoundGenerator(
        generators=[
            LineGenerator("ty", "mm", 0, 1, 2),
            LineGenerator("tx", "mm", 1, 2, 3),
        ],
        duration=0.5,
    )
    scan.configure(dict(generator=generator))
    with patch("bluefly.pmac.logic.BATCH_SIZE", 4):
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
        assert m.call_count == 5
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
