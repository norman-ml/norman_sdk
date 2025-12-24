from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.output_format import OutputFormat

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.signature_config import SignatureConfig


class TestModelVersionConfig:

    def test_create_with_all_fields(self) -> None:
        asset = AssetConfig(asset_name="model_weights.pt", data=b"binary weights", source=InputSource.Primitive)
        input_signature = SignatureConfig(display_title="Text Input", data_modality="text", data_domain="prompt", data_encoding="utf8", receive_format=ReceiveFormat.Primitive, parameters=[])
        output_signature = SignatureConfig(display_title="Text Output", data_modality="text", data_domain="response", data_encoding="utf8", receive_format=ReceiveFormat.Primitive, parameters=[])
        config = ModelVersionConfig(label="v1.0", short_description="Text classifier", long_description="A model for classifying text", assets=[asset], inputs=[input_signature], outputs=[output_signature], hosting_location=ModelHostingLocation.External, model_type=ModelType.Api, request_type=HttpRequestType.Post, url="https://api.example.com/predict", output_format=OutputFormat.Json, http_headers={"Authorization": "Bearer token"})

        assert config.label == "v1.0"
        assert config.short_description == "Text classifier"
        assert config.long_description == "A model for classifying text"
        assert len(config.assets) == 1
        assert len(config.inputs) == 1
        assert len(config.outputs) == 1
        assert config.hosting_location == ModelHostingLocation.External
        assert config.model_type == ModelType.Api
        assert config.request_type == HttpRequestType.Post
        assert config.url == "https://api.example.com/predict"
        assert config.output_format == OutputFormat.Json
        assert config.http_headers == {"Authorization": "Bearer token"}

    def test_create_without_optional_fields(self) -> None:
        config = ModelVersionConfig(label="v1.0", short_description="Image classifier", long_description="A CNN for image classification", assets=[], inputs=[], outputs=[])

        assert config.hosting_location is None
        assert config.model_type is None
        assert config.request_type is None
        assert config.url is None
        assert config.output_format is None
        assert config.http_headers is None

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in ModelVersionConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"label", "short_description", "long_description", "assets", "inputs", "outputs"}
