bluefly
=======

This module contains some experiments in writing flyscan Devices for
bluesky

Installation
------------

Once an initial release has been made you will be able to::

    pip install bluefly

Architecture
------------

This project aims to implement the minimum amount needed to demonstrate
fly scanning in the style of Malcolm, updated to use type hints and asyncio.
It separates interface classes from logic functions, and provides a
generic FlyDevice that wraps user-written FlyLogic classes.

FlyDevice
~~~~~~~~~

Let's start at the top. The interface up to bluesky is a "flyable" Device
with the following interface:

.. code-block:: python

    fly_device = FlyDevice(detectors=..., MyFlyLogic(...))
    # This means "tell your devices you are about to take data"
    # Detectors flag that they should open a new file, but don't
    # do anything about it yet
    fly_device.stage()
    # Store the flyscan trajectory and duration of each point
    # Doesn't need to be done many times if the trajectory is the same
    generator = CompoundGenerator(
        generators=[
            LineGenerator("ty", "mm", 0, 1, 2),
            LineGenerator("tx", "mm", 1, 2, 20),
        ],
        duration=0.1,
    )
    fly_device.configure(dict(generator=generator))
    # Actually open the file as this is the first kickoff
    await fly_device.kickoff()
    # We can now get the HDF file as a resource
    list(fly_device.collect())
    list(fly_device.collect_asset_docs())
    # Actually configure the hardware and start the fly_device.
    # Motors will move to the start, detectors will open files
    # and arm, then the fly_device will start. Status supports progress bar
    status = fly_device.complete()
    ...
    # Ctrl-C twice will do this, stop motors
    fly_device.pause()
    # Flag the fly_device as resuming from the last complete point, rather than
    # from the beginning
    fly_device.resume()
    # This has nothing to do as the file is already open
    await fly_device.kickoff()
    # Retrace to the last complete point, then trigger the resume of the fly_device
    status = fly_device.complete()
    ...
    # We can then trigger the fly_device more times to write more data
    await fly_device.kickoff()
    await fly_device.complete()
    await fly_device.kickoff()
    await fly_device.complete()
    # At any point we can collect the cached data and docs from the device
    list(fly_device.collect())
    list(fly_device.collect_asset_docs())
    # When we're done we trigger a file close
    fly_device.unstage()

This device can be nested inside other scans. This allows for instance a series
of flying mapping scans within a step scanned energy scan (using a custom per_step
function rather than one_nd_step)

The FlyDevice handles opening and closing the detectors, the FlyLogic handles
moving motors, arming and triggering detectors.

Writing the logic
~~~~~~~~~~~~~~~~~

Each flyscan has an element of uniqueness, so writing FlyLogic should be simple
and readable. The ``scan()`` method should start from a given point, placing
data at an offset in the file, calling back when any detector has produced frames.
The ``stop()`` method is used on pause to stop motion, triggering and detectors.
Implementations make heavy use of descriptive library functions that are mixed
together to make a set of logic:

.. literalinclude:: ../bluefly/fly.py
   :pyobject: PMACMasterFlyLogic

Note, the baseclass FlyLogic is an interface class with only NotImplemented methods.

DetectorDevice
~~~~~~~~~~~~~~

Detectors are SWMR HDF writing devices that are step scannable as well as being
used within flyscans. They are backed
by DetectorLogic that can be used both in step scans and fly scans. The ``open()``
method opens an HDF file in the given directory, returning details of the datasets
it will write. The ``get_deadtime()`` method returns the deadtime needed between
triggers for a given exposure time. The ``arm()`` method arms the detector for a
number of frames at a given exposure to be placed at a given offset in the file,
in software, triggered or gated mode. The ``collect()`` method then waits until
the given number of frames have been collected, giving periodic progress updates
via a callback. The ``stop()`` method stops the detector, and the ``close()``
method closes the file.

Again here, implementations make heavy use of library functions, so writing an
implementation for each detector shouldn't be too verbose:

.. literalinclude:: ../bluefly/areadetector.py
   :pyobject: AndorLogic

Note, the baseclass DetectorLogic is an interface class with only NotImplemented methods.

When using a DetectorDevice in a step scan, it reads the summary data back from
the HDF file to pass back to bluesky:

.. code-block:: python

    andor_logic = areadetector.AndorLogic(
        areadetector.DetectorDriver("BLxxI-EA-DET-01:DRV"),
        areadetector.HDFWriter("BLxxI-EA-DET-01:HDF5"),
    )
    andor = detector.DetectorDevice(andor_logic)
    # Flag that the HDF file needs opening on next trigger
    andor.stage()
    # Open the file, then arm and collect a single frame from the detector,
    # status support progress bar
    status = andor.trigger()
    ...
    # Data and shape read from HDF file
    andor.describe()
    andor.read()
    list(andor.collect_asset_docs())
    # Take more data after moving motors etc.
    await andor.trigger()
    andor.read()
    await andor.trigger()
    andor.read()
    list(andor.collect_asset_docs())
    # Close the HDF file
    andor.unstage()


MotorDevice
~~~~~~~~~~~

Motors are again similar to ophyd. They are not strictly needed for fly scanning
as we make use of a trajectory scan interface on the PMAC, but they are convenient
for tying a name to a motor record. It turns out to be easy to make them step
scannable too, so that is also added. They take a motor record class, but can
wrap classes like PMACRawMotor with extra signals.

HasSignals
~~~~~~~~~~

The lowest level objects in bluefly (DetectorDriver, HDFWriter, MotorRecord, PMACCoord)
are just containers for Signals. There is a utility class that lets them be defined
like this:

.. literalinclude:: ../bluefly/motor.py
   :pyobject: MotorRecord

This makes the Signals friendly for mypy checking, but requires some extra data to map
them to an actual Signal. This extra data is stored in the SignalProvider, and that
is what fills in the Signals in the actual instance. Mapping is generally one to one,
except in special cases where a dictionary mapping of several PVs to a single Signal
can be specified:

.. literalinclude:: ../bluefly/pmac.py
   :pyobject: PMACCoord

SignalProvider
~~~~~~~~~~~~~~

These are responsible for taking a signal prefix, the attribute names for the signals,
and their type, and filling in a HasSignals structure with concrete instances:

.. literalinclude:: ../bluefly/core.py
   :pyobject: SignalProvider

For instance,
``provider.make_signals("BLxxI-MO-TABLE-01:X", {"demand": ..., "readback"...})``
would provide a dictionary of Signals with entries "demand" and "readback" which can
be set as attributes of the instance.

The mapping from signal prefix and attribute name to PV, is done on a provider
specific way. I envisage that the CA and PVA structures can be done from PVI_.
This will expose a single PV with a JSON structure of the PVs (or pairs of PVs) of
interest in a Device, with name, description, label, widget, and any other metadata
that can't be got live from EPICS. It is in the progress of being added to areaDetector.

.. _PVI: https://pvi.readthedocs.io/en/latest/

Startup script
~~~~~~~~~~~~~~

These are all brought together in a typical bluesky startup script:

.. literalinclude:: ../tests/startup.py
   :start-at: # Running in simulation mode
   :end-before: # Run a step scan

There are a number of Context Managers active while the Devices are defined, which
follow a design pattern of defining an instance that can be stored and interacted
with for the duration of the with statement:

- SignalCollector lets SignalProviders be registered with optional transport prefixes
  like ``ca://`` to allow signal prefixes to be routed to the correct provider. The
  HasSignals baseclass uses this to fill in the correct Signals
- NamedDevices sets device.name to its name as defined in locals()
- TmpFilenameScheme() is a concrete filename scheme to allow file_prefixes to be
  created for each scan according to a local scheme

After the definitions, we have a number of bits of simulated logic being added. The
advantage of the interface classes is that simulations can be written at a Signal
level rather than having to reimplement any logic. For instance a PMAC trajectory scan:

.. literalinclude:: ../bluefly/pmac_sim.py
   :pyobject: sim_trajectory_logic

Demo
~~~~

Running ``pipenv run ipython -i -- test/startup.py`` will run first a step scan of
a simulated detector (with live plotting), then a flyscan of the same range. Data
is written from both, but I can't get plotting to work from the flyscan yet.

.. literalinclude:: ../tests/startup.py
   :start-at: # Run a step scan

