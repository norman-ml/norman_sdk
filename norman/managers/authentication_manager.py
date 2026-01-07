from datetime import datetime, timezone
from typing import Optional

import jwt
from norman_core.clients.http_client import HttpClient
from norman_core.services.authenticate import Authenticate
from norman_objects.services.authenticate.login.api_key_login_request import ApiKeyLoginRequest
from norman_objects.services.authenticate.signup.signup_key_request import SignupKeyRequest
from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_objects.shared.security.sensitive import Sensitive
from norman_utils_external.key_utils import KeyUtils
from norman_utils_external.singleton import Singleton


class AuthenticationManager(metaclass=Singleton):
    def __init__(self) -> None:
        self._authentication_service = Authenticate()
        self._http_client = HttpClient()

        self._api_key = None
        self._account_id = None
        self.__access_token: Optional[Sensitive[str]] = None
        self._id_token: Optional[Sensitive[str]] = None
        self._public_key: Optional[str] = None

    @property
    def access_token(self) -> Sensitive[str]:
        if self.__access_token is None:
            raise ValueError("Access token is not available — you may need to log in first")
        return self.__access_token

    @property
    def account_id(self) -> Optional[str]:
        return self._account_id

    def set_api_key(self, api_key: str) -> None:
        self._api_key = api_key

    @staticmethod
    async def signup_and_generate_key(username: str) -> SignupKeyResponse:
        async with HttpClient():
            authentication_service = Authenticate() # because signup_and_generate_key() is a static method
            signup_request = SignupKeyRequest(name=username)
            signup_response = await authentication_service.signup.signup_and_generate_key(signup_request)
            return signup_response

    async def invalidate_access_token(self) -> None:
        await self.__fetch_and_cache_public_key()

        if self.access_token_expired():
            await self.__login_with_api_key()

    async def __fetch_and_cache_public_key(self) -> None:
        if self._public_key is not None:
            return

        async with HttpClient():
            jwks = await self._authentication_service.jwks.get_key_set()

        if jwks is not None:
            jwks_dict = jwks.model_dump()
            jwk_list = jwks_dict["key_set"]
            self._public_key = KeyUtils.jwks_to_public_key(jwk_list)

    def access_token_expired(self) -> bool:
        if self.__access_token is None:
            return True

        try:
            if self._public_key is not None:
                decoded = jwt.decode(
                    self.__access_token.value(),
                    self._public_key,
                    algorithms=["RS256"],
                    audience="norman:server"
                )
            else:
                raise ValueError("Public key is required for access token verification")

            exp = decoded["exp"]
            now = datetime.now(timezone.utc).timestamp()
            return exp < now
        except Exception:
            return True

    async def __login_with_api_key(self) -> None:
        async with self._http_client:
            if self._api_key is None or self._api_key == "":
                raise ValueError("API key is required. Please provide a valid API key")

            login_request = ApiKeyLoginRequest(api_key=Sensitive(self._api_key))
            login_response = await self._authentication_service.login.login_with_key(login_request)

            self._account_id = login_response.account.id
            self.__access_token = login_response.access_token
            self._id_token = login_response.id_token

    async def logout(self) -> None:
        if self.__access_token is not None:
            async with HttpClient():
                await self._authentication_service.logout.logout(self.__access_token)

                self.__access_token = None
                self._id_token = None
