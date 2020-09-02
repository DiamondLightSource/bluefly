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

Let's start at the top. The interface up to bluesky is a ReadableDevice
with the following interface:

.. codeblock:: python

    scan = FlyScanDevice(MyFlyScanLogic(...))
    # Do this in a scan
    generator = CompoundGenerator(
        generators=[
            LineGenerator("ty", "mm", 0, 1, 2),
            LineGenerator("tx", "mm", 1, 2, 20),
        ],
        duration=0.1,
    )
    scan.configure(dict(generator=generator))
    # RE will do this, scan starts from the beginning
    status = scan.trigger()
    # Ctrl-C twice will do this, stop motors and retrace to the end
    # of the last completed step
    scan.pause()
    scan.resume()
    scan.trigger()


 that
supports pause and resume, can be configured with a scanpointgenerator
CompoundGenerator
