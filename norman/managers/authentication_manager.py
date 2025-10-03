from datetime import datetime, timezone
from typing import Optional

import jwt
from norman_core.clients.http_client import HttpClient
from norman_core.services.authenticate import Authenticate
from norman_objects.services.authenticate.login.account_id_password_login_request import AccountIDPasswordLoginRequest
from norman_objects.services.authenticate.login.api_key_login_request import ApiKeyLoginRequest
from norman_objects.services.authenticate.login.email_password_login_request import EmailPasswordLoginRequest
from norman_objects.services.authenticate.login.login_response import LoginResponse
from norman_objects.services.authenticate.login.name_password_login_request import NamePasswordLoginRequest
from norman_objects.services.authenticate.register.register_auth_factor_request import RegisterAuthFactorRequest
from norman_objects.shared.accounts.account import Account
from norman_objects.shared.security.sensitive import Sensitive

from norman.helpers.credentials_state import CredentialsState
from norman_objects.services.authenticate.register.register_email_request import RegisterEmailRequest
from norman_objects.services.authenticate.signup.signup_password_request import SignupPasswordRequest
from norman_objects.services.authenticate.signup.signup_email_request import SignupEmailRequest
from norman_objects.services.authenticate.register.resend_email_verification_code_request import ResendEmailVerificationCodeRequest

from norman_objects.services.authenticate.register.register_password_request import RegisterPasswordRequest

class AuthenticationManager:
    def __init__(self, credentials: CredentialsState):
        self._credentials = credentials
        self._token: Optional[Sensitive[str]] = None
        self._account: Optional[Account] = None

    @property
    def token(self) -> Optional[Sensitive[str]]:
        return self._token

    @property
    def account(self) -> Optional[Account]:
        return self._account

    @property
    def token_expired(self) -> bool:
        if self._token is None:
            return True
        try:
            decoded = jwt.decode(self._token.value(), options={"verify_signature": False})
            exp = decoded["exp"]
            now = datetime.now(timezone.utc).timestamp()
            return exp < (now + 300)  # expire 5 min early
        except Exception:
            return True

    def _update_session_state(self, response: LoginResponse, **cred_updates):
        if cred_updates:
            self._credentials.update(**cred_updates)
        self._token = response.access_token
        self._account = response.account
        return response

    # ------------------------------
    # Public login flows
    # ------------------------------
    async def login_default(self, account_id: str) -> LoginResponse:
        async with HttpClient() as http_client:
            response = await Authenticate.login.login_default(http_client, account_id=account_id)
        return self._update_session_state(response, account_id=account_id)

    async def login_with_password(self, account_id: str, password: str) -> LoginResponse:
        async with HttpClient() as http_client:
            request = AccountIDPasswordLoginRequest(account_id=account_id, password=Sensitive(password))
            response = await Authenticate.login.login_password_account_id(http_client, request)
        return self._update_session_state(response, account_id=account_id, password=password)

    async def login_with_api_key(self, account_id: str, api_key: str) -> LoginResponse:
        async with HttpClient() as http_client:
            request = ApiKeyLoginRequest(account_id=account_id, api_key=Sensitive(api_key))
            response = await Authenticate.login.login_with_key(http_client, request)
        return self._update_session_state(response, account_id=account_id, api_key=api_key)

    async def login_with_username_password(self, username: str, password: str) -> LoginResponse:
        async with HttpClient() as http_client:
            request = NamePasswordLoginRequest(name=username, password=Sensitive(password))
            response = await Authenticate.login.login_password_name(http_client, request)
        return self._update_session_state(response, username=username, password=password)

    async def login_with_email_password(self, email: str, password: str) -> LoginResponse:
        async with HttpClient() as http_client:
            request = EmailPasswordLoginRequest(email=email, password=Sensitive(password))
            response = await Authenticate.login.login_password_email(http_client, request)
        return self._update_session_state(response, email=email, password=password)

    async def login_email_otp(self, email: str):
        async with HttpClient() as http_client:
            await Authenticate.login.login_email_otp(http_client, email)

    async def verify_email_otp(self, email: str, code: str) -> LoginResponse:
        async with HttpClient() as http_client:
            response = await Authenticate.login.verify_email_otp(http_client, email, code)
            return self._update_session_state(response, email=email)

    # ------------------------------
    # Internal/private login helper
    # ------------------------------
    async def _login_internal(self, http_client: HttpClient) -> LoginResponse:
        if self._credentials.account_id and self._credentials.password:
            request = AccountIDPasswordLoginRequest(account_id=self._credentials.account_id, password=self._credentials.password)
            response = await Authenticate.login.login_password_account_id(http_client, request)
        elif self._credentials.account_id and self._credentials.api_key:
            request = ApiKeyLoginRequest(account_id=self._credentials.account_id, api_key=self._credentials.api_key)
            response = await Authenticate.login.login_with_key(http_client, request)
        elif self._credentials.username and self._credentials.password:
            request = NamePasswordLoginRequest(name=self._credentials.username, password=self._credentials.password)
            response = await Authenticate.login.login_password_name(http_client, request)
        elif self._credentials.email and self._credentials.password:
            request = EmailPasswordLoginRequest(email=self._credentials.email, password=self._credentials.password)
            response = await Authenticate.login.login_password_email(http_client, request)
        else:
            if not self._account:
                raise ValueError("No stored credentials available, and no account available for default login.")

            try:
                response = await self.login_default(account_id=self._account.id)
                return response
            except Exception as e:
                print(f"Failed to login default: {e}")
                raise ValueError("No stored credentials available, please log in.")

        return self._update_session_state(response)

    # ------------------------------
    # Public register flows
    # ------------------------------
    async def generate_api_key(self) -> str:
        if not self._token:
            raise RuntimeError("Must be logged in before generating an API key")

        async with HttpClient() as http_client:
            prev_token = self._token
            await self._login_internal(http_client)

            req = RegisterAuthFactorRequest(account_id=self._account.id, second_token=prev_token)
            return await Authenticate.register.generate_api_key(http_client, self._token, req)

    async def register_password(self, password: str):
        if not self._token:
            raise RuntimeError("Must be logged in before generating an API key")

        async with HttpClient() as http_client:
            prev_token = self._token
            await self._login_internal(http_client)

            request = RegisterPasswordRequest(account_id=self._account.id, password=Sensitive(password), second_token=prev_token)
            response = await Authenticate.register.register_password(http_client, self._token, request)
            return response

    async def register_email(self, email: str):
        if not self._token:
            raise RuntimeError("Must be logged in before generating an API key")

        async with HttpClient() as http_client:
            prev_token = self._token
            await self._login_internal(http_client)

            request = RegisterEmailRequest(account_id=self._account.id, email=email, second_token=prev_token)
            response = await Authenticate.register.register_email(http_client, self._token, request)
            return response

    async def verify_email(self, email: str, code: str):
        if not self._token:
            raise RuntimeError("Must be logged in before generating an API key")

        async with HttpClient() as http_client:
            await Authenticate.register.verify_email(http_client, self._token, email, code)

    async def resend_email_otp(self, email: str):
        if not self._token:
            raise RuntimeError("Must be logged in before generating an API key")

        async with HttpClient() as http_client:
            prev_token = self._token
            await self._login_internal(http_client)

            request = ResendEmailVerificationCodeRequest(account_id=self._account.id, email=email, second_token=prev_token)
            await Authenticate.register.verify_email(http_client, self._token, request)

    # ------------------------------
    # Public sign up flows
    # ------------------------------
    async def signup_default(self) -> LoginResponse:
        async with HttpClient() as http_client:
            response = await Authenticate.signup.signup_default(http_client)
            return self._update_session_state(response)

    async def signup_with_password(self, name: str, password: str) -> LoginResponse:
        async with HttpClient() as http_client:
            request = SignupPasswordRequest(name=name, password=Sensitive(password))
            response = await Authenticate.signup.signup_with_password(http_client, request)
            self._account = response
            return response

    async def signup_with_email(self, name: str, email: str) -> LoginResponse:
        async with HttpClient() as http_client:
            request = SignupEmailRequest(name=name, email=email)
            response = await Authenticate.signup.signup_with_email(http_client, request)
            self._account = response
            return response
