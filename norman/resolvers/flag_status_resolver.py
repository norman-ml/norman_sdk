import asyncio
import time
from typing import Sequence

from norman_core.services.persist import Persist
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

from norman._app_config import NormanAppConfig


class FlagStatusResolver:
    def __init__(self):
        self._persist_service = Persist()
        self._timeout_seconds =  NormanAppConfig.flag_timeout_seconds

    async def wait_for_entities(self, token: Sensitive[str], entity_ids: Sequence[str]) -> None:
        flag_constraints = QueryConstraints.includes("Status_Flags", "Entity_ID", list(entity_ids))
        wait_start_time = time.time()
        wait_end_time = wait_start_time + self._timeout_seconds

        while time.time() < wait_end_time:
            iteration_start_time = time.time()

            status_flags = await self._persist_service.status_flags.get_status_flags(token, flag_constraints)
            if status_flags is None:
                raise ValueError("No status flags found for entities")

            flattened_flags = []
            for flag_list in status_flags.values():
                flattened_flags += flag_list

            all_flags_finished = True
            for status_flag in flattened_flags:
                if status_flag.flag_value == StatusFlagValue.Error:
                    raise ValueError("Status flags at error state - One or more entities have failed")
                if status_flag.flag_value != StatusFlagValue.Finished:
                    all_flags_finished = False

            if all_flags_finished:
                return

            loop_iteration_end = time.time()
            iteration_duration = loop_iteration_end - iteration_start_time
            wait_time = NormanAppConfig.get_flags_interval - iteration_duration
            if wait_time > 0:
                await asyncio.sleep(wait_time)

        raise TimeoutError("Status flags did not finish - Timed out waiting for entities")
