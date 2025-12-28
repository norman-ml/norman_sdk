import base64
import uuid
from typing import AsyncGenerator, Generator, Optional

import pytest
import pytest_asyncio
from unittest.mock import PropertyMock, patch, MagicMock

from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.parameters.data_modality import DataModality
from norman_utils_external.name_utils import NameUtils

from norman import Norman
from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from norman.services.file_transfer_service import FileTransferService


from tests.constants import (
    ENCODING_UTF8,
    LOGO_VINCITEXT,
    MODEL_CATEGORY_QA,
    MODEL_FILE_TEXT,
    MODEL_FILES_DIR,
    MODEL_LOGOS_DIR,
    MODEL_TAGS_DEFAULT,
    TEST_ACCOUNT_ID,
    VERSION_LONG_DESC,
    VERSION_SHORT_DESC,
)

@pytest.fixture(scope="function")
def account_id_mock() -> Generator[PropertyMock, None, None]:
    with patch.object(
        AuthenticationManager,
        "account_id",
        new_callable=PropertyMock,
    ) as mock:
        mock.return_value = TEST_ACCOUNT_ID
        yield mock

def generate_unique_suffix() -> str:
    generated_uuid = uuid.uuid1()
    time_bytes = generated_uuid.time.to_bytes(8, byteorder="big")
    return base64.urlsafe_b64encode(time_bytes).decode("utf-8").rstrip("=")

@pytest_asyncio.fixture(scope="session")
async def api_key() -> AsyncGenerator[str, None]:
    try:
        account_name: str = NameUtils.generate_account_name()
        signup_response: SignupKeyResponse = await AuthenticationManager.signup_and_generate_key(
            account_name
        )
        yield signup_response.api_key
    except Exception as e:
        print(f"An error occurred while generating an api key: {e}")
        raise

@pytest_asyncio.fixture(scope="session")
async def authenticated_manager() -> AsyncGenerator[AuthenticationManager, None]:
    auth_manager: Optional[AuthenticationManager] = None
    try:
        account_name: str = NameUtils.generate_account_name()
        signup_response: SignupKeyResponse = await AuthenticationManager.signup_and_generate_key(
            account_name
        )

        auth_manager = AuthenticationManager()
        auth_manager.set_api_key(signup_response.api_key)

        yield auth_manager
    except Exception as e:
        print(f"An error occurred while wiring up an authentication manager: {e}")
        raise
    finally:
        if auth_manager is not None:
            await auth_manager.logout()

def build_test_model_config() -> dict:
    unique_suffix: str = generate_unique_suffix()

    file_asset = AssetConfig(
        asset_name="File",
        data=str(MODEL_FILES_DIR / MODEL_FILE_TEXT),
    )

    logo_asset = AssetConfig(
        asset_name="Logo",
        data=str(MODEL_LOGOS_DIR / LOGO_VINCITEXT),
    )

    text_input_parameter = ParameterConfig(
        parameter_name="raw_text",
        data_encoding=ENCODING_UTF8,
    )

    text_input_signature = SignatureConfig(
        display_title="Original text",
        data_modality=DataModality.Text,
        data_domain="prompt",
        data_encoding=ENCODING_UTF8,
        receive_format=ReceiveFormat.File,
        parameters=[text_input_parameter],
    )

    text_output_parameter = ParameterConfig(
        parameter_name="reverse_text",
        data_encoding=ENCODING_UTF8,
    )

    text_output_signature = SignatureConfig(
        display_title="Reversed text",
        data_modality=DataModality.Text,
        data_domain="llm_slop",
        data_encoding=ENCODING_UTF8,
        receive_format=ReceiveFormat.File,
        parameters=[text_output_parameter],
    )

    model_version = ModelVersionConfig(
        label=f"Version Aleph {unique_suffix}",
        short_description=VERSION_SHORT_DESC,
        long_description=VERSION_LONG_DESC,
        assets=[file_asset, logo_asset],
        inputs=[text_input_signature],
        outputs=[text_output_signature],
    )

    tag_configs: list[ModelTagConfig] = [ModelTagConfig(name=tag) for tag in MODEL_TAGS_DEFAULT]

    model_config = ModelProjectionConfig(
        name=f"VinciText SDK {unique_suffix}",
        category=MODEL_CATEGORY_QA,
        version=model_version,
        user_tags=tag_configs,
    )

    return model_config.model_dump()

@pytest.fixture(scope="function", autouse=False)
def file_transfer_service_reset() -> Generator[None, None, None]:
    """Reset FileTransferService singleton between tests."""
    FileTransferService._instances = {}
    yield
    FileTransferService._instances = {}

@pytest.fixture(scope="function")
def file_push_mock() -> Generator[MagicMock, None, None]:
    """Mock the FilePush class."""
    with patch("norman.services.file_transfer_service.FilePush") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance

class UploadedModelResult:
    def __init__(self, model: ModelProjection, config: dict, norman: Norman) -> None:
        self.model = model
        self.config = config
        self.norman = norman

    @property
    def entity_ids(self) -> list[str]:
        entity_ids: list[str] = [self.model.version.id]
        entity_ids.extend([asset.id for asset in self.model.version.assets])
        return entity_ids

@pytest_asyncio.fixture(scope="session")
async def uploaded_model(api_key: str) -> AsyncGenerator[UploadedModelResult, None]:
    norman = Norman(api_key)
    model_config: dict = build_test_model_config()

    print("\n[Fixture] Uploading model...")
    model: ModelProjection = await norman.upload_model(model_config)
    print(f"[Fixture] Model uploaded: {model.id}")

    yield UploadedModelResult(model=model, config=model_config, norman=norman)
