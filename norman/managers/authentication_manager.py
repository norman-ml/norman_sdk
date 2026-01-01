from datetime import datetime, timezone
from typing import Optional
import base64

import jwt
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from norman_core.clients.http_client import HttpClient
from norman_core.services.authenticate import Authenticate
from norman_objects.services.authenticate.login.api_key_login_request import ApiKeyLoginRequest
from norman_objects.services.authenticate.signup.signup_key_request import SignupKeyRequest
from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_objects.shared.authorization.jwk import JWK
from norman_objects.shared.security.sensitive import Sensitive
from norman_utils_external.singleton import Singleton


class AuthenticationManager(metaclass=Singleton):
    def __init__(self) -> None:
        self._authentication_service = Authenticate()
        self._http_client = HttpClient()

        self._api_key = None
        self._account_id = None
        self._access_token: Optional[Sensitive[str]] = None
        self._id_token: Optional[Sensitive[str]] = None
        self._public_key: Optional[str] = None

    @property
    def access_token(self) -> Sensitive[str]:
        if self._access_token is None:
            raise ValueError("Access token is not available — you may need to log in first")
        return self._access_token

    @property
    def account_id(self) -> Optional[str]:
        return self._account_id

    def set_api_key(self, api_key: str) -> None:
        self._api_key = api_key

    @staticmethod
    def _decode_base64url(data: str) -> bytes:
        padding = 4 - len(data) % 4
        if padding != 4:
            data += '=' * padding
        return base64.urlsafe_b64decode(data)

    @staticmethod
    def _jwk_to_pem(jwk: JWK) -> str:
        modulus = int.from_bytes(AuthenticationManager._decode_base64url(jwk.n), byteorder='big')
        exponent = int.from_bytes(AuthenticationManager._decode_base64url(jwk.e), byteorder='big')

        public_numbers = RSAPublicNumbers(exponent, modulus)
        public_key = public_numbers.public_key(default_backend())

        encoded_pem_key = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        pem_key = encoded_pem_key.decode('utf-8')

        return pem_key

    async def _fetch_and_cache_public_key(self) -> None:
        if self._public_key is not None:
            return

        jwks = await self._authentication_service.jwks.get_jwks()
        if jwks is not None:
            self._public_key = self._jwk_to_pem(jwks[0])

    def access_token_expired(self) -> bool:
        if self._access_token is None:
            return True
        try:
            if self._public_key is not None:
                decoded = jwt.decode(
                    self._access_token.value(),
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

    @staticmethod
    async def signup_and_generate_key(username: str) -> SignupKeyResponse:
        async with HttpClient():
            authentication_service = Authenticate() # because signup_and_generate_key() is a static method
            signup_request = SignupKeyRequest(name=username)
            signup_response = await authentication_service.signup.signup_and_generate_key(signup_request)
            return signup_response

    async def _login_with_api_key(self) -> None:
        async with self._http_client:
            if self._api_key is None or self._api_key == "":
                raise ValueError("API key is required. Please provide a valid API key")

            login_request = ApiKeyLoginRequest(api_key=Sensitive(self._api_key))
            login_response = await self._authentication_service.login.login_with_key(login_request)

            self._account_id = login_response.account.id
            self._access_token = login_response.access_token
            self._id_token = login_response.id_token

            await self._fetch_and_cache_public_key()

    async def invalidate_access_token(self) -> None:
        if self.access_token_expired():
            await self._login_with_api_key()

    async def logout(self) -> None:
        if self._access_token is not None:
            async with HttpClient():
                await self._authentication_service.logout.logout(self._access_token)

                self._access_token = None
                self._id_token = None
