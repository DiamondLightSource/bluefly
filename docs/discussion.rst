Discussion points
=================

Bluefly was written to explore some ideas on flyscanning in bluesky. This page lists
the discussion points that were discovered in its development.

Catching CancelledError in Status.success causes resume to fail
---------------------------------------------------------------

Status wraps an asyncio Task. If it is cancelled, success should be false. However,
checking the result and catching a CancelledError makes RE.resume() fail::

    ERROR:bluesky:Run aborted
    Traceback (most recent call last):
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/run_engine.py", line 1341, in _run
        msg = self._plan_stack[-1].throw(
    File "/home/tom/Programming/bluefly/tests/startup.py", line 111, in grid_fly
        yield from bps.sleep(1)
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/plan_stubs.py", line 450, in sleep
        return (yield Msg('sleep', None, time))
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/run_engine.py", line 1341, in _run
        msg = self._plan_stack[-1].throw(
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/utils.py", line 120, in <genexpr>
        gen = (msg for msg in gen)
    bluesky.utils.FailedStatus: <bluefly.core.Status object at 0x7f4b10126820>

Event loop passing
------------------

Early on we create the RE, but don't do anything with it. We then try to create some
asyncio primitives (Event, Queue) in the ipython main thread. This fails with:

    Got Future <Future pending> attached to a different loop

I think this is because bluesky creates an event loop but doesn't install it. We
can get round that by doing ``Event(loop=get_bluesky_event_loop())``, but this is
deprecated and will stop working in Python 3.10.


Some bluesky API methods could accept Status objects
----------------------------------------------------

Some methods are allowed to block the run-engine, like stage, pause, etc. It would be
good to return a Status object from these so that they could take time without blocking
the run-engine. The methods in question:

- stage
- pause
- resume
- unstage
- stop

Different use of flyer API
--------------------------

bluefly hooks the following methods to the flyer API:

- stage: Flag detectors to start a new file (return immediately)
- kickoff: Open detector files (on first run after stage)
- first collect: Yield resource
- complete: Move motors to start, arm detectors, start motion and triggers
- subsequent collects: Yield datums gathered so far
- unstage: Close detector files

The reason that kickoff doesn't actually start the motion is that it is easier to
write a single logic function that does both, and there is no reason to split the
two when a single flyer is in use.

To support subsequent collects, we need to periodically run collect while completing.
I can't find a way to do that with the current plan stubs. What I would like to do::

    yield from bps.complete(flyer, group="flyer")
    while True:
        try:
            # Wait for up to a second for the flyer to complete
            yield from bps.wait(group="flyer", timeout=1)
        except TimeoutError:
            # It hasn't completed yet
            yield from bps.collect(flyer, stream=True)
        else:
            # Flyer is done
            break
    # One last collect to make sure we got everything
    yield from bps.collect(flyer, stream=True)

Also, pause() worked when using the trigger and read interface, but with the flyer
interface it fails with::

    ERROR:bluesky:Run aborted
    Traceback (most recent call last):
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/run_engine.py", line 1341, in _run
        msg = self._plan_stack[-1].throw(
    File "/home/tom/Programming/bluefly/tests/startup.py", line 111, in grid_fly
        yield from bps.sleep(1)
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/plan_stubs.py", line 450, in sleep
        return (yield Msg('sleep', None, time))
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/run_engine.py", line 1341, in _run
        msg = self._plan_stack[-1].throw(
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/utils.py", line 120, in <genexpr>
        gen = (msg for msg in gen)
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/run_engine.py", line 1425, in _run
        new_response = await coro(msg)
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/run_engine.py", line 1891, in _collect
        return (await current_run.collect(msg))
    File "/home/tom/.local/share/virtualenvs/bluefly-1fc0e14f/lib/python3.8/site-packages/bluesky/bundlers.py", line 650, in collect
        seq_num = next(self._sequence_counters[stream_name])
    KeyError: 'primary'

There is also a question about what to do with rewind. Do we eagerly produce all data
up to bluesky, or is it better to lag behind in case a beam dump happens and we need
to rewind?


Data collection
---------------

At the moment bluefly detectors cache summary data out of the HDF files when
DetectorLogic provides updates. This could probably more sensibly be deferred until
collect or read. Reading data from the HDF file may slow down step scans a bit, but
most scanning at DLS is moving towards fly scanning, so implementing step and fly
scans in the same way makes most sense for us.

The emitting of Events rather than EventPages will not work for large datasets
(we are moving towards 100kHz or even 1MHz aquisitions of point detectors), so
this needs exploring further.

Documentation on collect_asset_docs was hard to find, it was mainly gleaned from
example code.

We have some detectors (like PandA) that write lots of datasets in a single HDF
file. We also need some additional kwargs (like tagging datasets as
detector/monitor) to allow NeXus files to be emitted. I assume the best way to
do this is to write a new Handler


ScanPointGenerator
------------------

Rather than specify flyscans as CompoundGenerators and outer scans as cyclers it
would be good to merge the two. This requires some more thought, but the basic idea
is to specify a scan as a serializable object, from which the following can be got:

- Number of points
- Names of the axes
- Range of the axes
- Dataset dimensionality (so a VDS can be created)
- API to get a Point or Points object (wrapping one or many scan points)
- Centre-point of each axis in Point (or array in Points)
- Upper and lower bounds of each axis in Point (or array in Points)
- Duration of Point (or array in Points)
- Time delay after Point (or array in Points)

Signal API
----------

The biggest change here from Ophyd is the definition of Signals. By making
get(), put() and observe() async, all code that uses them must be async.
Are there plans to go asyncio for Ophyd?

The other issue raised is the dynamic building of Signals at init. This
needs more discussion, as the definition of these HasSignals classes is
key to how the mypy checking works. Separating these out from the
Logic is also key (e.g. MotorDevice has a MotorRecord, rather than is a).

We also need to discuss monitor vs get. I favour making get() async,
and not monitoring in the backgroud. This doesn't rule out doing background
monitoring at the Logic level, but I think it should be at stage rather
than always on. This would avoid a lot of the workarounds we had to do in
Malcolm, but would stop you having to get the motor EGUs at each scan point.

