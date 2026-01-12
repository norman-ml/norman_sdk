import pytest

from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model_build_status import ModelBuildStatus
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.model_version import ModelVersion
from norman_objects.shared.models.output_format import OutputFormat

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.factories.model_version_factory import ModelVersionFactory
from tests.constants import DEFAULT_ID, DEFAULT_MODEL_ID, TEST_ACCOUNT_ID


@pytest.mark.unit
@pytest.mark.usefixtures("account_id_mock")
class TestModelVersionFactory:
    @pytest.mark.factory
    def test_create_returns_model_version_with_defaults(self) -> None:
        label = "v1.0"

        model_version_config = ModelVersionConfig(
            label=label,
            short_description="Test model",
            long_description="A test model",
            assets=[],
            inputs=[],
            outputs=[],
        )

        model_version = ModelVersionFactory.create(model_version_config)

        assert isinstance(model_version, ModelVersion)
        assert model_version.label == label
        assert model_version.id == DEFAULT_ID
        assert model_version.model_id == DEFAULT_MODEL_ID
        assert model_version.build_status == ModelBuildStatus.InProgress
        assert model_version.active is True
        assert model_version.model_type == ModelType.Pytorch_jit
        assert model_version.request_type == HttpRequestType.Post
        assert model_version.output_format == OutputFormat.Json
        assert model_version.hosting_location == ModelHostingLocation.Internal
        assert model_version.http_headers == {}

    @pytest.mark.factory
    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test",
            long_description="Test",
            assets=[],
            inputs=[],
            outputs=[],
        )

        model_version = ModelVersionFactory.create(model_version_config)

        assert model_version.account_id == TEST_ACCOUNT_ID

    @pytest.mark.factory
    def test_create_with_external_hosting_requires_url(self) -> None:
        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="External",
            long_description="External model",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            url=None,
        )

        with pytest.raises(ValueError, match="External models must define a url field"):
            ModelVersionFactory.create(model_version_config)

    @pytest.mark.factory
    def test_create_with_external_hosting_and_url(self) -> None:
        url = "https://api.example.com/predict"
        http_headers = {"Authorization": "Bearer token"}

        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="External",
            long_description="External model",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            url=url,
            http_headers=http_headers,
        )

        model_version = ModelVersionFactory.create(model_version_config)

        assert model_version.hosting_location == ModelHostingLocation.External
        assert model_version.url == url
        assert model_version.http_headers == http_headers

    @pytest.mark.factory
    def test_create_with_input_signature(self) -> None:
        input_display_title = "Text Input"

        signature_config = SignatureConfig(
            display_title=input_display_title,
            data_modality="text",
            data_domain="prompt",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            parameters=[],
        )
        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test",
            long_description="Test",
            assets=[],
            inputs=[signature_config],
            outputs=[],
        )

        model_version = ModelVersionFactory.create(model_version_config)

        assert len(model_version.inputs) == 1
        assert model_version.inputs[0].display_title == input_display_title

    @pytest.mark.factory
    def test_create_with_output_signature(self) -> None:
        output_display_title = "Text Output"

        signature_config = SignatureConfig(
            display_title=output_display_title,
            data_modality="text",
            data_domain="response",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            parameters=[],
        )
        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test",
            long_description="Test",
            assets=[],
            inputs=[],
            outputs=[signature_config],
        )

        model_version = ModelVersionFactory.create(model_version_config)

        assert len(model_version.outputs) == 1
        assert model_version.outputs[0].display_title == output_display_title

    @pytest.mark.factory
    def test_create_with_asset(self) -> None:
        asset_name = "weights.pt"

        asset_config = AssetConfig(
            asset_name=asset_name,
            data=b"binary data",
            source=InputSource.Primitive,
        )
        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test",
            long_description="Test",
            assets=[asset_config],
            inputs=[],
            outputs=[],
        )

        model_version = ModelVersionFactory.create(model_version_config)

        assert len(model_version.assets) == 1
        assert model_version.assets[0].asset_name == asset_name
