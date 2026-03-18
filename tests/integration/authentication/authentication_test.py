import pytest
from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_objects.shared.security.sensitive import Sensitive
from norman_utils.name_utils import NameUtils

from norman.managers.authentication_manager import AuthenticationManager


@pytest.mark.usefixtures("authentication_manager")
class TestAuthentication:

    @pytest.mark.authentication
    async def test_signup_and_generate_key(self) -> None:
        account_name: str = NameUtils.generate_account_name()
        signup_response: SignupKeyResponse = await AuthenticationManager.signup_and_generate_key(account_name)
        api_key: str = signup_response.api_key

        assert isinstance(api_key, str)
        assert len(api_key) == 112 # Do not explain structure in public code other than the length (which is already public).

    @pytest.mark.authentication
    def test_access_token_initially_expired(self, authentication_manager: AuthenticationManager) -> None:
        token_expired = authentication_manager.access_token_expired()

        assert token_expired

    @pytest.mark.authentication
    async def test_access_token_does_not_immediately_expire(self, authentication_manager: AuthenticationManager) -> None:
        await authentication_manager.invalidate_access_token()
        token_expired = authentication_manager.access_token_expired()

        assert not token_expired

    @pytest.mark.authentication
    async def test_invalidation_performs_login_and_creates_account_id(self, authentication_manager: AuthenticationManager) -> None:
        await authentication_manager.invalidate_access_token()
        account_id: str = authentication_manager.account_id

        assert isinstance(account_id, str)
        assert len(account_id) > 0

    @pytest.mark.authentication
    async def test_invalidation_performs_login_and_creates_access_token(self, authentication_manager: AuthenticationManager) -> None:
        await authentication_manager.invalidate_access_token()
        access_token: Sensitive[str] = authentication_manager.access_token

        assert isinstance(access_token, Sensitive)
        assert isinstance(access_token.value(), str)
        assert len(access_token.value()) > 0
