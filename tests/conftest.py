import base64
import os
import uuid
from pathlib import Path
from typing import AsyncGenerator, Optional

import pytest_asyncio
from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
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

def build_test_model_config() -> dict:
    generated_uuid = uuid.uuid1()
    uuid_time = generated_uuid.time
    time_bytes = uuid_time.to_bytes(8, byteorder="big")
    time_base64 = base64.urlsafe_b64encode(time_bytes).decode("utf-8").rstrip("=")

    file_asset = AssetConfig(
        asset_name="File",
        data=os.sep.join([str(Norman_Test_Root), "assets", "model_files", "text_qa_model.pt"])
    )

    logo_asset = AssetConfig(
        asset_name="Logo",
        data=os.sep.join([str(Norman_Test_Root), "assets", "model_logos", "Vincitext_logo.jpg"])
    )

    text_input_parameter = ParameterConfig(
        parameter_name="raw_text",
        data_encoding="utf8",
    )

    text_input_signature = SignatureConfig(
        display_title="Original text",
        data_modality=DataModality.Text,
        data_domain="prompt",
        data_encoding="utf8",
        receive_format=ReceiveFormat.File,
        parameters=[text_input_parameter]
    )

    text_output_parameter = ParameterConfig(
        parameter_name="reverse_text",
        data_encoding="utf8",
    )

    text_output_signature = SignatureConfig(
        display_title="Reversed text",
        data_modality=DataModality.Text,
        data_domain="llm_slop",
        data_encoding="utf8",
        receive_format=ReceiveFormat.File,
        parameters=[text_output_parameter]
    )

    version_config = ModelVersionConfig(
        label=f"Version Aleph {time_base64}",
        short_description="An end to end quality assurance model, used to test the model upload process through our SDK.",
        long_description="This language model can also be used during inference to test the input and output signature processing of text models. Simply write some text and have it transformed by this genuine AI model.",
        assets=[file_asset, logo_asset],
        inputs=[text_input_signature],
        outputs=[text_output_signature]
    )

    first_tag_config = ModelTagConfig(name="SLM")
    second_tag_config = ModelTagConfig(name="QA")
    third_tag_config = ModelTagConfig(name="Test")

    model_config = ModelProjectionConfig(
        name=f"VinciText SDK {time_base64}",
        category="QA Model",
        version=version_config,
        user_tags=[first_tag_config, second_tag_config, third_tag_config]
    )

    return model_config.model_dump()


class UploadedModelResult:
    def __init__(self, model, config: dict, norman: Norman):
        self.model = model
        self.config = config
        self.norman = norman

    @property
    def entity_ids(self) -> list[str]:
        entity_ids = [self.model.version.id]
        entity_ids.extend([asset.id for asset in self.model.version.assets])
        return entity_ids

@pytest_asyncio.fixture(scope="session")
async def uploaded_model(api_key: str) -> AsyncGenerator[UploadedModelResult, None]:
    norman = Norman(api_key)
    model_config = build_test_model_config()

    print(f"\n[Fixture] Uploading model...")
    model = await norman.upload_model(model_config)
    print(f"[Fixture] Model uploaded: {model.id}")

    result = UploadedModelResult(model=model, config=model_config, norman=norman)

    yield result
