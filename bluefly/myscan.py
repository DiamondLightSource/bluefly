from typing import Dict

from scanpointgenerator.core.compoundgenerator import CompoundGenerator

from bluefly.scan import FlyScanLogic


class MyFlyScanLogic(FlyScanLogic):
    def __init__(self, pmac_id: str, detector_ids: Dict[str, str]):
        self._pmac_id = pmac_id
        self._detector_ids = detector_ids

    async def go(self, generator: CompoundGenerator):
        pass
