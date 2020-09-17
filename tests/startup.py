import bluesky.plans as bp
from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.utils import ProgressBarManager, install_kicker
from databroker import Broker
from IPython import get_ipython
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

RE = RunEngine({})

bec = BestEffortCallback()

# Send all metadata/data captured to the BestEffortCallback.
RE.subscribe(bec)

# Make plots update live while scans run.
install_kicker()
get_ipython().magic("matplotlib qt")

# Create a databroker backed by temporary files
db = Broker.named("mycat")


def spy(name, doc):
    if name == "resource":
        print(doc)


# Insert all metadata/data captured into db.
RE.subscribe(db.insert)
RE.subscribe(spy)

# Make a progress bar
RE.waiting_hook = ProgressBarManager()

# Running in simulation mode?
SIM_MODE = True

with SignalCollector(), NamedDevices(), TmpFilenameScheme():
    # All Signals with a sim:// prefix or without a prefix will come from this provider
    if SIM_MODE:
        sim = SignalCollector.add_provider(sim=SimProvider(), set_default=True)
    else:
        # Do something like this here
        # ca = SignalCollector.add_provider(ca=CAProvider(), set_default=True)
        pass
    # A PMAC has a trajectory scan interface and 16 Co-ordinate systems
    # which may have motors in them
    pmac1 = pmac.PMAC("BLxxI-MO-PMAC-01:")
    # Raw motors assigned to a single CS, settable for use in step scans
    t1x = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:X"))
    t1y = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Y"))
    t1z = motor.MotorDevice(pmac.PMACRawMotor("BLxxI-MO-TABLE-01:Z"))
    # Simulated detector
    andor_logic = areadetector.AndorLogic(
        areadetector.DetectorDriver("BLxxI-EA-DET-01:DRV"),
        areadetector.HDFWriter("BLxxI-EA-DET-01:HDF5"),
    )
    andor = detector.DetectorDevice(andor_logic)
    # Define a flyscan that can move any combination of these 3 motors which
    # are required to be in the same CS on the pmac
    mapping = fly.FlyDevice([andor], fly.PMACMasterFlyLogic(pmac1, [t1x, t1y, t1z]))
    # Signals are connected (in a blocking way) at the end of the with block
    # and all the Devices in locals() have their names filled in

# Fill in the simulated logic
if SIM_MODE:
    pmac_sim.sim_trajectory_logic(sim, pmac1.traj, a=t1x, b=t1y)
    for m in (t1x, t1y, t1z):
        motor_sim.sim_motor_logic(sim, m)
    areadetector_sim.sim_detector_logic(
        sim, andor_logic.driver, andor_logic.hdf, t1x, t1y
    )

# Run a step scan
RE(bp.grid_scan([andor], t1x, 3, 5, 10, t1y, 2, 4, 8))

# Run a fly scan
generator = CompoundGenerator(
    generators=[
        LineGenerator("t1y", "mm", 2, 4, 8),
        LineGenerator("t1x", "mm", 3, 5, 10),
    ],
    duration=0.1,
)
mapping.configure(dict(generator=generator))
RE(bp.fly([mapping]))
