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
would provide a dictionary of Signals
