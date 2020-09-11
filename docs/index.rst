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
generic FlyScanDevice that wraps user-written FlyScanLogic classes.

FlyScanDevice
~~~~~~~~~~~~~

Let's start at the top. The interface up to bluesky is a ReadableDevice
with the following interface:

.. code-block:: python

    fly_device = FlyScanDevice(MyFlyScanLogic(detectors=..., motor=...))
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
    # Actually configure the hardware and start the fly_device.
    # Motors will move to the start, detectors will open files
    # and arm, then the fly_device will start. Status supports progress bar
    status = fly_device.trigger()
    ...
    # Ctrl-C twice will do this, stop motors
    fly_device.pause()
    # Flag the fly_device as resuming from the last point, rather than from
    # the beginning
    fly_device.resume()
    # Retrace to the last good step, then trigger the resume of the fly_device
    fly_device.trigger()
    ...
    # We can then trigger the fly_device more times to write more data
    await fly_device.trigger()
    await fly_device.trigger()
    # When we're done we trigger a file close
    fly_device.unstage()

This device can be nested inside other scans. This allows for instance a series
of flying mapping scans within a step scanned energy scan.

Writing the logic
~~~~~~~~~~~~~~~~~

Each flyscan has an element of uniqueness, so is backed by Logic that can do the
stages of

DetectorDevice
~~~~~~~~~~~~~~

Detectors are SWMR HDF writing devices that are step scannable. They are backed


FlyScanLogic
~~~~~~~~~~~~

Flyscans typically have

MotorDevice
~~~~~~~~~~~

Moving down a level, we have the concepts of Motors. A Motor is
like an ophyd EpicsMotor, it can be step scanned, and produces its value
on read(). However, a trajectory scan is not performed at the level of a single
motor, so it must be wrapped into