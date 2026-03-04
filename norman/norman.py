from typing import Any, List

from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_objects.shared.capacity.capacity_remaining import CapacityRemaining
from norman_objects.shared.models.model_projection import ModelProjection
from norman_utils_external.singleton import Singleton

from norman.managers.authentication_manager import AuthenticationManager
from norman.managers.capacity_manager import CapacityManager
from norman.managers.invocation_manager import InvocationManager
from norman.managers.model_upload_manager import ModelUploadManager


class Norman(metaclass=Singleton):
    def __init__(self, api_key: str):
        self._authentication_manager = AuthenticationManager()
        self._authentication_manager.set_api_key(api_key)

        self._capacity_manager = CapacityManager()
        self._invocation_manager = InvocationManager()
        self._model_upload_manager = ModelUploadManager()

    @staticmethod
    async def signup(username: str) -> SignupKeyResponse:
        return await AuthenticationManager.signup_and_generate_key(username)

    async def get_remaining_capacity(self) -> List[CapacityRemaining]:
        return await self._capacity_manager.get_remaining_capacity()

    async def upload_model(self, model_config: dict[str, Any]) -> ModelProjection:
        return await self._model_upload_manager.upload_model(model_config)

    async def upgrade_model(self, model_config: dict[str, Any]) -> ModelProjection:
        return await self._model_upload_manager.upgrade_model(model_config)

    async def invoke(self, invocation_config: dict[str, Any]) -> dict[str, bytes]:
        return await self._invocation_manager.invoke(invocation_config)