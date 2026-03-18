from pathlib import Path
from typing import AsyncGenerator, Optional

import pytest_asyncio
from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_utils.name_utils import NameUtils

from norman.managers.authentication_manager import AuthenticationManager


Norman_Test_Root = Path(__file__).parent.resolve()
Norman_Library_Root = Norman_Test_Root.parent

@pytest_asyncio.fixture(scope="session")
async def authentication_manager() -> AsyncGenerator[AuthenticationManager, None]:
    authentication_manager: Optional[AuthenticationManager] = None
    try:
        account_name: str = NameUtils.generate_account_name()
        signup_response: SignupKeyResponse = await AuthenticationManager.signup_and_generate_key(account_name)
        api_key: str = signup_response.api_key

        authentication_manager = AuthenticationManager()
        authentication_manager.set_api_key(api_key)

        yield authentication_manager
    except Exception as e:
        print("An error occurred while wiring up an authentication manager")
        print(e)
    finally:
        if authentication_manager is not None:
            await authentication_manager.logout()

@pytest_asyncio.fixture(scope="session")
async def api_key() -> AsyncGenerator[str, None]:
    try:
        account_name: str = NameUtils.generate_account_name()
        signup_response: SignupKeyResponse = await AuthenticationManager.signup_and_generate_key(account_name)
        api_key: str = signup_response.api_key

        yield api_key
    except Exception as e:
        print("An error occurred while generating an api key")
        print(e)
