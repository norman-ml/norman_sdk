import base64
import os
import uuid

import pytest
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.parameters.data_modality import DataModality

from norman import Norman
from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from tests.conftest import Norman_Test_Root


@pytest.mark.usefixtures("api_key")
class TestModelUpload:
    @pytest.mark.models
    async def test_create_model(self, api_key: str) -> None:
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
            name="VinciText SDK",
            category="QA Model",
            version=version_config,
            user_tags=[first_tag_config, second_tag_config, third_tag_config]
        )

        raw_model_config = model_config.model_dump()

        norman = Norman(api_key)
        model = await norman.upload_model(raw_model_config)

        print(model.model_dump())

    @pytest.mark.models
    def test_upload_model_configuration(self, authentication_manager: AuthenticationManager) -> None:
        pass

    @pytest.mark.models
    def test_upload_model_asset(self, authentication_manager: AuthenticationManager) -> None:
        pass

    @pytest.mark.models
    def test_wait_for_status_flags(self, authentication_manager: AuthenticationManager) -> None:
        pass
