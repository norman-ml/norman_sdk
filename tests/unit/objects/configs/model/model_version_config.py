import pytest

from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.output_format import OutputFormat

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.signature_config import SignatureConfig


@pytest.mark.unit
@pytest.mark.config
class TestModelVersionConfig:
    def test_create_with_all_fields(self) -> None:
        label = "v1.0"
        short_description = "Text classifier"
        long_description = "A model for classifying text"
        url = "https://api.example.com/predict"
        http_headers = {"Authorization": "Bearer token"}

        asset_config = AssetConfig(
            asset_name="model_weights.pt",
            data=b"binary weights",
            source=InputSource.Primitive,
        )
        input_signature = SignatureConfig(
            display_title="Text Input",
            data_modality="text",
            data_domain="prompt",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            parameters=[],
        )
        output_signature = SignatureConfig(
            display_title="Text Output",
            data_modality="text",
            data_domain="response",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            parameters=[],
        )
        model_version = ModelVersionConfig(
            label=label,
            short_description=short_description,
            long_description=long_description,
            assets=[asset_config],
            inputs=[input_signature],
            outputs=[output_signature],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            request_type=HttpRequestType.Post,
            url=url,
            output_format=OutputFormat.Json,
            http_headers=http_headers,
        )

        assert model_version.label == label
        assert model_version.short_description == short_description
        assert model_version.long_description == long_description
        assert len(model_version.assets) == 1
        assert len(model_version.inputs) == 1
        assert len(model_version.outputs) == 1
        assert model_version.hosting_location == ModelHostingLocation.External
        assert model_version.model_type == ModelType.Api
        assert model_version.request_type == HttpRequestType.Post
        assert model_version.url == url
        assert model_version.output_format == OutputFormat.Json
        assert model_version.http_headers == http_headers

    def test_create_without_optional_fields(self) -> None:
        model_version = ModelVersionConfig(
            label="v1.0",
            short_description="Image classifier",
            long_description="A CNN for image classification",
            assets=[],
            inputs=[],
            outputs=[],
        )

        assert model_version.hosting_location is None
        assert model_version.model_type is None
        assert model_version.request_type is None
        assert model_version.url is None
        assert model_version.output_format is None
        assert model_version.http_headers is None

    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in ModelVersionConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {
            "label",
            "short_description",
            "long_description",
            "assets",
            "inputs",
            "outputs",
        }
