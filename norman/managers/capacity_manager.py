import asyncio
from typing import List

from norman_core.clients.http_client import HttpClient
from norman_core.services.authenticate import Authenticate
from norman_core.services.persist import Persist
from norman_objects.shared.capacity.capacity_remaining import CapacityRemaining
from norman_objects.shared.queries.query_constraints import QueryConstraints

from norman.managers.authentication_manager import AuthenticationManager


class CapacityManager:
    def __init__(self) -> None:
        self._authentication_manager = AuthenticationManager()
        self._authenticate_service = Authenticate()
        self._persist_service = Persist()
        self._http_client = HttpClient()

    async def get_remaining_capacity(self) -> List[CapacityRemaining]:
        await self._authentication_manager.invalidate_access_token()

        async with self._http_client:
            account_id = self._authentication_manager.account_id
            account_capacity, capacity_usage = await self.__get_capacity_and_usage(account_id)

            return self.__calculate_remaining_capacity(account_id, account_capacity, capacity_usage)

    async def __get_capacity_and_usage(self, account_id: str):
        account_capacity_constraints = QueryConstraints.equals("Account_Capacity", "Account_ID", account_id)
        capacity_usage_constraints = QueryConstraints.equals("Capacity_Usage", "Account_ID", account_id)

        account_capacity, capacity_usage = await asyncio.gather(
            self._authenticate_service.capacity.get_account_capacity(token=self._authentication_manager.access_token, constraints=account_capacity_constraints),
            self._persist_service.capacity_usage.get_capacity_usage(token=self._authentication_manager.access_token, constraints=capacity_usage_constraints)
        )

        return account_capacity, capacity_usage

    def __calculate_remaining_capacity(self, account_id, account_capacity, capacity_usage) -> List[CapacityRemaining]:
        aggregate_account_capacity = {}
        for capacity in account_capacity:
            if capacity.machine_type not in aggregate_account_capacity:
                aggregate_account_capacity[capacity.machine_type] = 0
            aggregate_account_capacity[capacity.machine_type] += capacity.capacity

        aggregate_capacity_usage = {}
        for usage in capacity_usage:
            if usage.machine_type not in aggregate_capacity_usage:
                aggregate_capacity_usage[usage.machine_type] = 0
            aggregate_capacity_usage[usage.machine_type] += usage.capacity

        remaining_capacity = []
        for machine_type in aggregate_account_capacity:
            if machine_type not in aggregate_capacity_usage:
                used_capacity = 0
            else:
                used_capacity = aggregate_capacity_usage[machine_type]

            remaining_capacity.append(
                CapacityRemaining(
                    account_id=account_id,
                    machine_type=machine_type,
                    capacity=aggregate_account_capacity[machine_type] - used_capacity
                )
            )

        return remaining_capacity