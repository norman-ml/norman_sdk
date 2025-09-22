import asyncio
from typing import Optional, Any

from norman import Norman
from norman.managers._invocation_manager import InvocationTracker
from norman.managers._upload_manager import UploadTracker
from norman.objects.configs.invocation_config import InvocationConfig


class SyncNorman:
    def __init__(self, api_key: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None):
        self.__norman = Norman(api_key, username, password)

    def invoke(self, invocation_config: InvocationConfig, *, progress_tracker: Optional[InvocationTracker] = None):
        return asyncio.run(self.__norman.invoke(invocation_config, progress_tracker=progress_tracker))

    def upload_model(self, model_config: dict[str, Any], *, progress_tracker: Optional[UploadTracker] = None):
        return asyncio.run(self.__norman.upload_model(model_config, progress_tracker=progress_tracker))

    def generate_api_key(self):
        return asyncio.run(self.__norman.generate_api_key())

    def create_user(self, username: str, password):
        return asyncio.run(self.__norman.create_user(username, password))

    def update_auth_factors(
            self,
            account_id: str = None,
            username: str = None,
            email: str = None,
            password: str = None,
            api_key: str = None
        ):
        self.__norman.update_auth_factors(account_id, username, email, password, api_key)
