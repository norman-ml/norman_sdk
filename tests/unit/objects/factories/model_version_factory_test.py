from unittest.mock import patch, PropertyMock

import pytest
from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model_asset import ModelAsset
from norman_objects.shared.models.model_build_status import ModelBuildStatus
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.model_version import ModelVersion
from norman_objects.shared.models.output_format import OutputFormat
from norman_objects.shared.parameters.data_modality import DataModality

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.factories.model_version_factory import ModelVersionFactory

SIGNATURE_FACTORY_PATH = "norman.objects.factories.model_version_factory.SignatureFactory"
ASSET_FACTORY_PATH = "norman.objects.factories.model_version_factory.AssetFactory"


@pytest.fixture
def mock_account_id():
    with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock:
        mock.return_value = "account-123"
        yield mock


@pytest.fixture
def mock_signature_factory():
    with patch(SIGNATURE_FACTORY_PATH) as mock:
        yield mock


@pytest.fixture
def mock_asset_factory():
    with patch(ASSET_FACTORY_PATH) as mock:
        yield mock


def create_mock_signature(signature_type: SignatureType) -> ModelSignature:
    return ModelSignature(id="0", model_id="0", version_id="0", signature_type=signature_type, data_modality=DataModality.Audio, data_domain="speech", data_encoding="wav", receive_format=ReceiveFormat.Primitive, http_location=HttpLocation.Body, hidden=False, display_title="Audio Signature", default_value=None, parameters=[], transforms=[], signature_args={})


def create_mock_asset(asset_name: str) -> ModelAsset:
    return ModelAsset(id="0", account_id="account-123", model_id="0", version_id="0", asset_name=asset_name)


@pytest.mark.usefixtures("mock_account_id", "mock_signature_factory", "mock_asset_factory")
class TestModelVersionFactory:

    def test_create_returns_model_version_with_defaults(self) -> None:
        config = ModelVersionConfig(label="v1.0", short_description="Test model", long_description="A test model", assets=[], inputs=[], outputs=[])

        created_version = ModelVersionFactory.create(config)

        assert isinstance(created_version, ModelVersion)
        assert created_version.label == "v1.0"
        assert created_version.id == "0"
        assert created_version.model_id == "0"
        assert created_version.build_status == ModelBuildStatus.InProgress
        assert created_version.active is True
        assert created_version.model_type == ModelType.Pytorch_jit
        assert created_version.request_type == HttpRequestType.Post
        assert created_version.output_format == OutputFormat.Json
        assert created_version.hosting_location == ModelHostingLocation.Internal
        assert created_version.http_headers == {}

    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        config = ModelVersionConfig(label="v1.0", short_description="Test", long_description="Test", assets=[], inputs=[], outputs=[])

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = "account-456"

            created_version = ModelVersionFactory.create(config)

            assert created_version.account_id == "account-456"

    def test_create_with_external_hosting_requires_url(self) -> None:
        config = ModelVersionConfig(label="v1.0", short_description="External", long_description="External model", assets=[], inputs=[], outputs=[], hosting_location=ModelHostingLocation.External, url=None)

        with pytest.raises(ValueError, match="External models must define a url field"):
            ModelVersionFactory.create(config)

    def test_create_with_external_hosting_and_url(self) -> None:
        config = ModelVersionConfig(label="v1.0", short_description="External", long_description="External model", assets=[], inputs=[], outputs=[], hosting_location=ModelHostingLocation.External, model_type=ModelType.Api, url="https://api.example.com/predict", http_headers={"Authorization": "Bearer token"})

        created_version = ModelVersionFactory.create(config)

        assert created_version.hosting_location == ModelHostingLocation.External
        assert created_version.url == "https://api.example.com/predict"
        assert created_version.http_headers == {"Authorization": "Bearer token"}

    def test_create_calls_signature_factory_for_inputs_and_outputs(self) -> None:
        signature_config = SignatureConfig(display_title="Input", data_modality="text", data_domain="prompt", data_encoding="utf8", receive_format=ReceiveFormat.Primitive, parameters=[])
        config = ModelVersionConfig(label="v1.0", short_description="Test", long_description="Test", assets=[], inputs=[signature_config], outputs=[signature_config])

        with patch(SIGNATURE_FACTORY_PATH) as mock_factory:
            mock_factory.create.side_effect = [create_mock_signature(SignatureType.Input), create_mock_signature(SignatureType.Output)]

            created_version = ModelVersionFactory.create(config)

            assert mock_factory.create.call_count == 2
            assert len(created_version.inputs) == 1
            assert len(created_version.outputs) == 1

    def test_create_calls_asset_factory_for_assets(self) -> None:
        asset_config = AssetConfig(asset_name="weights.pt", data=b"binary data", source=InputSource.Primitive)
        config = ModelVersionConfig(label="v1.0", short_description="Test", long_description="Test", assets=[asset_config], inputs=[], outputs=[])

        with patch(ASSET_FACTORY_PATH) as mock_factory:
            mock_factory.create.return_value = create_mock_asset("weights.pt")

            created_version = ModelVersionFactory.create(config)

            mock_factory.create.assert_called_once_with(asset_config)
            assert len(created_version.assets) == 1
