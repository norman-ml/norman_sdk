from typing import Any

from norman_core.clients.http_client import HttpClient
from norman_core.services.authenticate import Authenticate
from norman_core.services.persist import Persist
from norman_objects.shared.queries.query_constraints import QueryConstraints

from norman.managers.authentication_manager import AuthenticationManager


class CapacityManager:
    def __init__(self) -> None:
        self._authentication_manager = AuthenticationManager()
        self._authenticate_service = Authenticate()
        self._persist_service = Persist()
        self._http_client = HttpClient()

    async def get_remaining_capacity(self, capacity_config: dict[str, Any]) -> dict[str, int]:
        model_id = capacity_config["model_id"]
        version_id = capacity_config["version_id"]

        await self._authentication_manager.invalidate_access_token()
        token = self._authentication_manager.access_token

        async with self._http_client:
            account_id = self._authentication_manager.account_id
            account_constraints = QueryConstraints.equals("Account_Capacity", "Account_ID", account_id)
            account_capacity = await self._authenticate_service.capacity.get_account_capacity(token=token, constraints=account_constraints)

            constraints = (
                QueryConstraints.equals("Capacity_Usage", "Model_ID", model_id)
                & QueryConstraints.equals("Capacity_Usage", "Version_ID", version_id)
            )

            capacity_usage = await self._persist_service.capacity_usage.get_capacity_usage(token=token, constraints=constraints)

        usage_by_machine_type = {}
        for usage in capacity_usage:
            usage_by_machine_type[usage.machine_type] = usage_by_machine_type.get(usage.machine_type, 0) + usage.capacity

        remaining_capacity = {}
        for capacity in account_capacity:
            used_capacity = usage_by_machine_type.get(capacity.machine_type, 0)
            remaining_capacity[capacity.machine_type] = capacity.capacity - used_capacity

        return remaining_capacity