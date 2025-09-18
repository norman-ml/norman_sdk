import base64
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Any

from norman_core.clients.http_client import HttpClient
from norman_core.services.authenticate import Authenticate
from norman_objects.services.authenticate.login.api_key_login_request import ApiKeyLoginRequest
from norman_objects.services.authenticate.login.name_password_login_request import NamePasswordLoginRequest
from norman_objects.shared.accounts.account import Account
from norman_objects.shared.security.sensitive import Sensitive

from norman._invocation_manager import InvocationManager, InvocationTracker
from norman._upload_manager import UploadManager, UploadTracker
from norman.norman_types import InvocationConfig


class Norman:
    def __init__(self, api_key: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None):
        if api_key is None and (username is None or password is None):
            raise ValueError("Either api_key or username and password must be provided")

        if api_key is not None:
            self.__api_key = Sensitive(api_key)
        else:
            self.__api_key = None
        if password is not None:
            self.__password = Sensitive(password)
        else:
            self.__password = None

        self.__username = username

        self.__token: Optional[Sensitive[str]] = None
        self.__account: Optional[Account] = None

    @asynccontextmanager
    async def __get_http_client(self):
        http_client = HttpClient()
        if self.token_expired:
            await self.__connect(http_client)
        yield http_client
        await http_client.close()

    @property
    def token_expired(self) -> bool:
        if self.__token is None:
            return True

        try:
            _, payload, _ = self.__token.value().split('.')
            payload += '=' * ((4 - len(payload) % 4) % 4)
            decoded_payload = base64.urlsafe_b64decode(payload).decode('utf-8')
            decoded = json.loads(decoded_payload)
            now = datetime.now(timezone.utc).timestamp()

            exp = decoded.get("exp")
            extra_time_seconds = 300
            if exp is None or exp < (now + extra_time_seconds):
                return True

            return False

        except (ValueError, AttributeError):
            return True

    async def __connect(self, http_client: HttpClient):
        if self.__api_key is not None:
            login_request = ApiKeyLoginRequest(api_key=self.__api_key, account_id="why?")
            login_response = await Authenticate.login.login_with_key(http_client, login_request)
        else:
            login_request = NamePasswordLoginRequest(name=self.__username, password=self.__password)
            login_response = await Authenticate.login.login_password_name(http_client, login_request)

        self.__token = login_response.access_token
        self.__account = login_response.account

    async def invoke(self, invocation_config: InvocationConfig, *, progress_tracker: Optional[InvocationTracker] = None):
        async with self.__get_http_client() as http_client:
            invocation_manager = InvocationManager(http_client, self.__token, invocation_config, progress_tracker)
            await invocation_manager.create_invocation()
            await invocation_manager.upload_inputs()
            await invocation_manager.wait_for_flags()
            results = await invocation_manager.get_results()
            return results

    async def upload_model(self, model_config: dict[str, Any], *, progress_tracker: Optional[UploadTracker] = None):
        async with self.__get_http_client() as http_client:
            upload_manager = UploadManager(http_client, self.__token, self.__account.id, model_config, progress_tracker=progress_tracker)
            await upload_manager.upload_model()
            await upload_manager.upload_assets()
            await upload_manager.wait_for_flags()
            return upload_manager.model
