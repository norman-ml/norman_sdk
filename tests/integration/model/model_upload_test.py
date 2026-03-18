import base64
import os
import uuid

import pytest
from norman_objects.shared.encoding.channel_encoding import ChannelEncoding
from norman_objects.shared.encoding.container_encoding import ContainerEncoding
from norman_objects.shared.encoding.sample_encoding import SampleEncoding
from norman_objects.shared.encoding.tensor_encoding import TensorEncoding
from norman_objects.shared.modality.channel_modality import ChannelModality
from norman_objects.shared.modality.container_modality import ContainerModality
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat

from norman import Norman
from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from pathlib import Path

Norman_Test_Root = Path(__file__).resolve().parents[3]


@pytest.mark.usefixtures("api_key")
class TestModelUpload:
    @pytest.mark.models
    async def test_create_model(self, api_key: str) -> None:
        generated_uuid = uuid.uuid1()
        uuid_time = generated_uuid.time
        time_bytes = uuid_time.to_bytes(8, byteorder="big")
        time_base64 = base64.urlsafe_b64encode(time_bytes).decode("utf8").rstrip("=")

        file_asset = AssetConfig(
            asset_name="File",
            data=os.sep.join([str(Norman_Test_Root), "tests", "assets", "model_files", "text_qa_model.pt"])
        )

        logo_asset = AssetConfig(
            asset_name="Logo",
            data=os.sep.join([str(Norman_Test_Root), "tests", "assets", "model_logos", "Vincitext_logo.jpg"])
        )

        text_input_parameter = ParameterConfig(
            channel_modality=ChannelModality.Text,
            channel_encoding=ChannelEncoding.Utf8,
            sample_encoding=SampleEncoding.U8,
            tensor_encoding=TensorEncoding.Int64,
            name="raw_text"
        )

        text_input_signature = SignatureConfig(
            display_title="Original text",
            container_modality=ContainerModality.Text,
            data_domain="prompt",
            container_encoding=ContainerEncoding.Txt,
            receive_format=ReceiveFormat.File,

            parameters=[text_input_parameter]
        )

        text_output_parameter = ParameterConfig(
            channel_modality=ChannelModality.Text,
            channel_encoding=ChannelEncoding.Utf8,
            sample_encoding=SampleEncoding.U8,
            tensor_encoding=TensorEncoding.Int64,
            name="reverse_text"
        )

        text_output_signature = SignatureConfig(
            display_title="Reversed text",
            container_modality=ContainerModality.Text,
            data_domain="llm_slop",
            container_encoding=ContainerEncoding.Txt,
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
            name="VinciText50 SDK",
            category="QA Model",
            version=version_config,
            user_tags=[first_tag_config, second_tag_config, third_tag_config]
        )

        raw_model_config = model_config.model_dump()

        norman = Norman(api_key)
        model = await norman.upload_model(raw_model_config)

    @pytest.mark.models
    async def test_upgrade_model(self, api_key: str) -> None:
        generated_uuid = uuid.uuid1()
        uuid_time = generated_uuid.time
        time_bytes = uuid_time.to_bytes(8, byteorder="big")
        time_base64 = base64.urlsafe_b64encode(time_bytes).decode("utf-8").rstrip("=")

        file_asset = AssetConfig(
            asset_name="File",
            data=os.sep.join([str(Norman_Test_Root), "tests", "assets", "model_files", "text_qa_model.pt"])
        )

        logo_asset = AssetConfig(
            asset_name="Logo",
            data=os.sep.join([str(Norman_Test_Root), "tests", "assets", "model_logos", "Vincitext_logo.jpg"])
        )

        text_input_parameter = ParameterConfig(
            channel_modality=ChannelModality.Text,
            channel_encoding=ChannelEncoding.Utf8,
            sample_encoding=SampleEncoding.U8,
            tensor_encoding=TensorEncoding.Int64,
            name="raw_text"
        )

        text_input_signature = SignatureConfig(
            display_title="Original text",
            container_modality=ContainerModality.Text,
            data_domain="prompt",
            container_encoding=ContainerEncoding.Txt,
            receive_format=ReceiveFormat.File,

            parameters=[text_input_parameter]
        )

        text_output_parameter = ParameterConfig(
            channel_modality=ChannelModality.Text,
            channel_encoding=ChannelEncoding.Utf8,
            sample_encoding=SampleEncoding.U8,
            tensor_encoding=TensorEncoding.Int64,
            name="reverse_text"
        )

        text_output_signature = SignatureConfig(
            display_title="Reversed text",
            container_modality=ContainerModality.Text,
            data_domain="llm_slop",
            container_encoding=ContainerEncoding.Txt,
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

        model_config = ModelProjectionConfig(
            name=f"VinciText50 SDK {time_base64}",
            category="QA Model",
            version=version_config
        )

        norman = Norman(api_key)
        model = await norman.upload_model(model_config.model_dump())

        upgrade_version_config = ModelVersionConfig(
            label=f"Version Bet {time_base64}",
            short_description="Upgraded version of the QA model.",
            long_description="This is an upgraded version used to test the model upgrade process through our SDK.",
            assets=[file_asset, logo_asset],
            inputs=[text_input_signature],
            outputs=[text_output_signature]
        )

        upgrade_model_config = ModelProjectionConfig(
            id=model.id,
            name=f"VinciText50 SDK {time_base64}",
            category="QA Model",
            version=upgrade_version_config,
        )

        upgraded_model = await norman.upgrade_model(upgrade_model_config.model_dump())

        assert upgraded_model.id == model.id
        assert upgraded_model.version.label == f"Version Bet {time_base64}"

    @pytest.mark.models
    def test_upload_model_configuration(self, authentication_manager: AuthenticationManager) -> None:
        pass

    @pytest.mark.models
    def test_upload_model_asset(self, authentication_manager: AuthenticationManager) -> None:
        pass

    @pytest.mark.models
    def test_wait_for_status_flags(self, authentication_manager: AuthenticationManager) -> None:
        pass
