from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.utils import install_kicker, ProgressBarManager
from databroker import Broker

from ophyd.sim import motor
from ophyd import StatusBase, Device


RE = RunEngine({})
bec = BestEffortCallback()

# Send all metadata/data captured to the BestEffortCallback.
RE.subscribe(bec)

# Make plots update live while scans run.
install_kicker()

# Create a databroker backed by temporary files
db = Broker.named('temp')

# Insert all metadata/data captured into db.
RE.subscribe(db.insert)

# Make a progress bar
RE.waiting_hook = ProgressBarManager()

