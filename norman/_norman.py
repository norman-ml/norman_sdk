import base64
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Any

from norman_core.clients.http_client import HttpClient
from norman_core.services.authenticate import Authenticate
from norman_objects.services.authenticate.login.account_id_password_login_request import AccountIDPasswordLoginRequest
from norman_objects.services.authenticate.login.api_key_login_request import ApiKeyLoginRequest
from norman_objects.services.authenticate.login.email_password_login_request import EmailPasswordLoginRequest
from norman_objects.services.authenticate.login.name_password_login_request import NamePasswordLoginRequest
from norman_objects.services.authenticate.register.register_auth_factor_request import RegisterAuthFactorRequest
from norman_objects.services.authenticate.signup.signup_password_request import SignupPasswordRequest
from norman_objects.shared.accounts.account import Account
from norman_objects.shared.security.sensitive import Sensitive

from norman._invocation_manager import InvocationManager, InvocationTracker
from norman._upload_manager import UploadManager, UploadTracker
from norman.norman_types import InvocationConfig


class Norman:
    def __init__(
            self,
            account_id: Optional[str] = None,
            username: Optional[str] = None,
            email: Optional[str] = None,
            password: Optional[str] = None,
            api_key: Optional[str] = None
        ):
        self.__account_id = account_id
        self.__username = username
        self.__email = email
        self.__password = self.__wrap_sensitive(password)
        self.__api_key = self.__wrap_sensitive(api_key)

        self.__token: Optional[Sensitive[str]] = None
        self.__account: Optional[Account] = None

    @staticmethod
    def __wrap_sensitive(sensitive: Optional[Any]):
        return Sensitive(sensitive) if sensitive is not None else None

    @asynccontextmanager
    async def __get_http_client(self, login=True):
        http_client = HttpClient("https://api.dev.avremy.public.norman-ai.com/v0")
        if login and self.__token_expired:
            await self.__login(http_client)
        yield http_client
        await http_client.close()

    @property
    def __token_expired(self) -> bool:
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

    async def __login(self, http_client: HttpClient):
        if self.__account_id and self.__password:
            login_request = AccountIDPasswordLoginRequest(account_id=self.__account_id, password=self.__password)
            login_response = await Authenticate.login.login_password_account_id(http_client, login_request)
        elif self.__account_id and self.__api_key:
            login_request = ApiKeyLoginRequest(api_key=self.__api_key, account_id=self.__account_id)
            login_response = await Authenticate.login.login_with_key(http_client, login_request)
        elif self.__username and self.__password:
            login_request = NamePasswordLoginRequest(name=self.__username, password=self.__password)
            login_response = await Authenticate.login.login_password_name(http_client, login_request)
        elif self.__email and self.__password:
            login_request = EmailPasswordLoginRequest(email=self.__email, password=self.__password)
            login_response = await Authenticate.login.login_password_email(http_client, login_request)
        else:
            raise ValueError("Invalid login combination provided")

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

    async def generate_api_key(self):
        async with self.__get_http_client() as http_client:
            token = self.__token
            await self.__login(http_client)
            register_api_key_request = RegisterAuthFactorRequest(account_id=self.__account.id, second_token=token)
            api_key: str = await Authenticate.register.generate_api_key(http_client, self.__token, register_api_key_request)
            return api_key

    async def create_user(self, username: str, password: str):
        async with self.__get_http_client(login=False) as http_client:
            signup_request = SignupPasswordRequest(name=username, password=password)
            login_response = await Authenticate.signup.signup_with_password(http_client, signup_request)
            return login_response

    def update_auth_factors(
            self,
            account_id: str = None,
            username: str = None,
            email: str = None,
            password: Sensitive[str] = None,
            api_key: Sensitive[str] = None
        ):
        self.__account_id = account_id or self.__account_id
        self.__username = username or self.__username
        self.__email = email or self.__email
        self.__password = self.__wrap_sensitive(password) or self.__password
        self.__api_key = self.__wrap_sensitive(api_key) or self.__api_key
