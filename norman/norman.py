from typing import Any, Coroutine

from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.models.model_tag import ModelTag
from norman_utils_external.singleton import Singleton

from norman.managers.authentication_manager import AuthenticationManager
from norman.managers.invocation_manager import InvocationManager
from norman.managers.model_upload_manager import ModelUploadManager
from norman.managers.tag_manager import TagManager


class Norman(metaclass=Singleton):
    def __init__(self, api_key: str):
        self._authentication_manager = AuthenticationManager()
        self._authentication_manager.set_api_key(api_key)

        self._invocation_manager = InvocationManager()
        self._model_upload_manager = ModelUploadManager()
        self._tag_manager = TagManager()

    @staticmethod
    async def signup(username: str) -> SignupKeyResponse:
        return await AuthenticationManager.signup_and_generate_key(username)

    async def upload_model(self, model_config: dict[str, Any]) -> ModelProjection:
        return await self._model_upload_manager.upload_model(model_config)

    async def invoke(self, invocation_config: dict[str, Any]) -> dict[str, bytes]:
        return await self._invocation_manager.invoke(invocation_config)

    async def tag_model(self, model_name: str, tag_name: str) -> ModelTag:
        return await self._tag_manager.tag_model(model_name, tag_name)

    async def untag_model(self, model_name: str, tag_name: str) -> int:
        return await self._tag_manager.untag_model(model_name, tag_name)

