from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.plans import count
from bluesky.utils import ProgressBarManager, install_kicker
from databroker import Broker
from scanpointgenerator import CompoundGenerator, LineGenerator

from bluefly.myscan import MyFlyScanLogic
from bluefly.pmac.sim import SimPMAC
from bluefly.scan import FlyScanDevice

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

# Make the device
sim_pmac = SimPMAC()
logic = MyFlyScanLogic(sim_pmac.motors, sim_pmac.cs_list, sim_pmac.traj)
scan = FlyScanDevice("scan", logic)

# Configure a scan
generator = CompoundGenerator(
    generators=[
        LineGenerator("ty", "mm", 0, 1, 2),
        LineGenerator("tx", "mm", 1, 2, 20),
    ],
    duration=0.1,
)
scan.configure(dict(generator=generator))

# Run a scan
RE(count([scan]))
