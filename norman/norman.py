from contextlib import asynccontextmanager
from typing import Any, Optional

from norman_core.clients.http_client import HttpClient
from norman_objects.shared.models.model import Model

from norman.helpers.credentials_state import CredentialsState
from norman.helpers.model_factory import ModelFactory
from norman.managers.authentication_manager import AuthenticationManager
from norman.managers.invocation_manager import InvocationManager
from norman.managers.model_upload_manager import ModelUploadManager
from norman.objects.configs.invocation_config import InvocationConfig


class Norman(AuthenticationManager):
    def __init__(
        self,
        account_id: Optional[str] = None,
        username: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None
    ):
        credentials = CredentialsState(
            account_id=account_id,
            username=username,
            email=email,
            password=password,
            api_key=api_key,
        )

        super().__init__(credentials)

    async def invoke(self, invocation_config: InvocationConfig) -> dict[str, bytearray]:
        async with self._get_http_client() as http_client:
            invocation = await InvocationManager.create_invocation_in_database(http_client, self.token, invocation_config)
            await InvocationManager.upload_inputs(http_client, self.token, invocation, invocation_config)
            await InvocationManager.wait_for_flags(http_client, self.token, invocation)
            return await InvocationManager.get_results(http_client, self.token, invocation)

    async def upload_model(self, model_config: dict[str, Any]) -> Model:
        async with self._get_http_client() as http_client:
            model = ModelFactory.create_model(self.account.id, model_config)
            model = await ModelUploadManager.upload_model(http_client, self.token, model)
            await ModelUploadManager.upload_assets(http_client, self.token, model, model_config["assets"])
            await ModelUploadManager.wait_for_flags(http_client, self.token, model)
            return model

    @asynccontextmanager
    async def _get_http_client(self, login=True):
        http_client = HttpClient()
        if login and self.token_expired:
            await self._login_internal(http_client)
        yield http_client
        await http_client.close()

