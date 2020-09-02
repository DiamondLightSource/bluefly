from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.plans import count
from bluesky.utils import ProgressBarManager, install_kicker
from databroker import Broker
from scanpointgenerator import CompoundGenerator, LineGenerator

from bluefly import pmac
from bluefly.core import SignalCollector
from bluefly.myscan import MyFlyScanLogic
from bluefly.scan import FlyScanDevice
from bluefly.simprovider import SimProvider

RE = RunEngine({})

bec = BestEffortCallback()

# Send all metadata/data captured to the BestEffortCallback.
RE.subscribe(bec)

# Make plots update live while scans run.
install_kicker()

# Create a databroker backed by temporary files
db = Broker.named("temp")

# Insert all metadata/data captured into db.
RE.subscribe(db.insert)

# Make a progress bar
RE.waiting_hook = ProgressBarManager()

with SignalCollector() as sc:
    # All Signals with a sim:// prefix or without a prefix will come from this provider
    sim = sc.add_provider(sim=SimProvider(), set_default=True)
    # A PMAC has a trajectory scan interface and 16 Co-ordinate systems
    # which may have motors in them
    pmac1 = pmac.PMAC("BLxxI-MO-PMAC-01:")
    # Raw motors assigned to a single CS
    t1x = pmac.PMACRawMotor("BLxxI-MO-TABLE-01:X")
    t1y = pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Y")
    t1z = pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Z")
    # Define a flyscan that can move any combination of these 3 motors which
    # are required to be in the same CS on the pmac
    scan = FlyScanDevice(MyFlyScanLogic(pmac1, [t1x, t1y, t1z]))
    # Signals are connected (in a blocking way) at the end of the with block
    # and all the Devices in locals() have their names filled in

# Fill in the simulated trajectory logic
pmac.sim_trajectory_logic(sim, pmac1.traj)

# Configure a scan
generator = CompoundGenerator(
    generators=[
        LineGenerator("t1y", "mm", 0, 1, 2),
        LineGenerator("t1x", "mm", 1, 2, 20),
    ],
    duration=0.1,
)
scan.configure(dict(generator=generator))

# Run a scan
RE(count([scan]))
