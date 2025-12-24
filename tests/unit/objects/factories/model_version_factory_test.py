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
from pydantic import ValidationError

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.factories.model_version_factory import ModelVersionFactory

SIGNATURE_FACTORY_PATH = "norman.objects.factories.model_version_factory.SignatureFactory"
ASSET_FACTORY_PATH = "norman.objects.factories.model_version_factory.AssetFactory"
MODEL_WEIGHTS_ASSET = "model-weights.pt"

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


@pytest.fixture
def sample_version_config() -> ModelVersionConfig:
    return ModelVersionConfig(
        label="v1.0",
        short_description="Speech recognition model",
        long_description="A neural network model for transcribing audio to text",
        assets=[],
        inputs=[],
        outputs=[],
        hosting_location=ModelHostingLocation.Internal,
        model_type=ModelType.Pytorch_jit,
        request_type=HttpRequestType.Post,
        url=None,
        output_format=OutputFormat.Json,
        http_headers=None
    )

@pytest.fixture
def sample_signature_config() -> SignatureConfig:
    return SignatureConfig(
        display_title="Audio Input",
        data_modality="audio",
        data_domain="speech",
        data_encoding="wav",
        receive_format=ReceiveFormat.Primitive,
        http_location=HttpLocation.Body,
        hidden=False,
        default_value=None,
        parameters=[]
    )

@pytest.fixture
def sample_asset_config() -> AssetConfig:
    return AssetConfig(asset_name=MODEL_WEIGHTS_ASSET, data=b"binary model weights", source=InputSource.Primitive)

def create_mock_signature(signature_type: SignatureType = SignatureType.Input) -> ModelSignature:
    return ModelSignature(
        id="0",
        model_id="0",
        version_id="0",
        signature_type=signature_type,
        data_modality=DataModality.Audio,
        data_domain="speech",
        data_encoding="wav",
        receive_format=ReceiveFormat.Primitive,
        http_location=HttpLocation.Body,
        hidden=False,
        display_title="Audio Signature",
        default_value=None,
        parameters=[],
        transforms=[],
        signature_args={}
    )


def create_mock_asset(asset_name: str = MODEL_WEIGHTS_ASSET) -> ModelAsset:
    return ModelAsset(
        id="0",
        account_id="account-123",
        model_id="0",
        version_id="0",
        asset_name=asset_name
    )


@pytest.mark.usefixtures("mock_account_id", "mock_signature_factory", "mock_asset_factory")
class TestModelVersionFactory:
    # --- Return Type and Basic Fields ---

    def test_create_returns_model_version_instance(self, sample_version_config: ModelVersionConfig) -> None:
        created_version = ModelVersionFactory.create(sample_version_config)

        assert isinstance(created_version, ModelVersion)

    def test_create_sets_label_from_config(self) -> None:
        beta_version_config = ModelVersionConfig(
            label="v2.0-beta",
            short_description="Beta release of image classifier",
            long_description="Experimental version with improved accuracy on edge cases",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(beta_version_config)

        assert created_version.label == "v2.0-beta"

    def test_create_sets_short_description_from_config(self) -> None:
        nlp_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Natural language processing model",
            long_description="Transformer-based model for text classification and sentiment analysis",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Transformer_hf,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(nlp_version_config)

        assert created_version.short_description == "Natural language processing model"

    def test_create_sets_long_description_from_config(self) -> None:
        detailed_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Image classification model",
            long_description="A comprehensive deep learning model for classifying images into 1000 categories using ResNet architecture",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(detailed_version_config)

        assert created_version.long_description == "A comprehensive deep learning model for classifying images into 1000 categories using ResNet architecture"

    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Account test model",
            long_description="Model for testing account ID assignment",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = "account-456"

            created_version = ModelVersionFactory.create(version_config)

            assert created_version.account_id == "account-456"

    # --- Fixed Values ---

    def test_create_sets_build_status_to_in_progress(self, sample_version_config: ModelVersionConfig) -> None:
        created_version = ModelVersionFactory.create(sample_version_config)

        assert created_version.build_status == ModelBuildStatus.InProgress

    def test_create_sets_active_to_true(self, sample_version_config: ModelVersionConfig) -> None:
        created_version = ModelVersionFactory.create(sample_version_config)

        assert created_version.active is True

    # --- Default Values ---

    def test_create_sets_default_id(self, sample_version_config: ModelVersionConfig) -> None:
        created_version = ModelVersionFactory.create(sample_version_config)

        assert created_version.id == "0"

    def test_create_sets_default_model_id(self, sample_version_config: ModelVersionConfig) -> None:
        created_version = ModelVersionFactory.create(sample_version_config)

        assert created_version.model_id == "0"

    # --- Request Type Handling ---

    def test_create_with_none_request_type_defaults_to_post(self) -> None:
        default_request_config = ModelVersionConfig(
            label="v1.0",
            short_description="Default request type model",
            long_description="Model testing default request type behavior",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=None,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(default_request_config)

        assert created_version.request_type == HttpRequestType.Post

    def test_create_preserves_explicit_request_type_get(self) -> None:
        get_request_config = ModelVersionConfig(
            label="v1.0",
            short_description="GET endpoint model",
            long_description="Model accessed via HTTP GET request for lightweight inference",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            request_type=HttpRequestType.Get,
            url="https://api.example.com/predict",
            output_format=OutputFormat.Json,
            http_headers={"Authorization": "Bearer token"}
        )

        created_version = ModelVersionFactory.create(get_request_config)

        assert created_version.request_type == HttpRequestType.Get

    def test_create_preserves_explicit_request_type_put(self) -> None:
        put_request_config = ModelVersionConfig(
            label="v1.0",
            short_description="PUT endpoint model",
            long_description="Model accessed via HTTP PUT request for stateful updates",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            request_type=HttpRequestType.Put,
            url="https://api.example.com/update",
            output_format=OutputFormat.Json,
            http_headers={"Authorization": "Bearer token"}
        )

        created_version = ModelVersionFactory.create(put_request_config)

        assert created_version.request_type == HttpRequestType.Put

    # --- Model Type Handling ---

    def test_create_with_none_model_type_defaults_to_pytorch_jit(self) -> None:
        default_model_type_config = ModelVersionConfig(
            label="v1.0",
            short_description="Default model type",
            long_description="Model testing default model type behavior",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=None,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(default_model_type_config)

        assert created_version.model_type == ModelType.Pytorch_jit

    def test_create_preserves_explicit_model_type_api(self) -> None:
        api_model_config = ModelVersionConfig(
            label="v1.0",
            short_description="External API model",
            long_description="Model hosted on external API endpoint",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            request_type=HttpRequestType.Post,
            url="https://api.openai.com/v1/completions",
            output_format=OutputFormat.Json,
            http_headers={"Authorization": "Bearer sk-xxx"}
        )

        created_version = ModelVersionFactory.create(api_model_config)

        assert created_version.model_type == ModelType.Api

    def test_create_preserves_explicit_model_type_transformer(self) -> None:
        transformer_config = ModelVersionConfig(
            label="v1.0",
            short_description="HuggingFace transformer model",
            long_description="BERT-based transformer model from HuggingFace model hub",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Transformer_hf,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(transformer_config)

        assert created_version.model_type == ModelType.Transformer_hf

    # --- Output Format Handling ---

    def test_create_with_none_output_format_defaults_to_json(self) -> None:
        default_output_config = ModelVersionConfig(
            label="v1.0",
            short_description="Default output format model",
            long_description="Model testing default output format behavior",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=None,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(default_output_config)

        assert created_version.output_format == OutputFormat.Json

    def test_create_preserves_explicit_output_format_binary(self) -> None:
        binary_output_config = ModelVersionConfig(
            label="v1.0",
            short_description="Binary output model",
            long_description="Image generation model that outputs raw binary data",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Binary,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(binary_output_config)

        assert created_version.output_format == OutputFormat.Binary

    def test_create_preserves_explicit_output_format_text(self) -> None:
        text_output_config = ModelVersionConfig(
            label="v1.0",
            short_description="Text output model",
            long_description="Language model that outputs plain text responses",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Transformer_hf,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Text,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(text_output_config)

        assert created_version.output_format == OutputFormat.Text

    # --- Hosting Location Handling ---

    def test_create_with_none_hosting_location_defaults_to_internal(self) -> None:
        default_hosting_config = ModelVersionConfig(
            label="v1.0",
            short_description="Default hosting model",
            long_description="Model testing default hosting location behavior",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=None,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(default_hosting_config)

        assert created_version.hosting_location == ModelHostingLocation.Internal

    def test_create_with_internal_hosting_sets_empty_url(self) -> None:
        internal_hosting_config = ModelVersionConfig(
            label="v1.0",
            short_description="Internal hosted model",
            long_description="Model hosted on internal infrastructure",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(internal_hosting_config)

        assert created_version.url == ""

    def test_create_with_external_hosting_and_url(self) -> None:
        external_hosting_config = ModelVersionConfig(
            label="v1.0",
            short_description="External hosted model",
            long_description="Model hosted on external cloud provider",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            request_type=HttpRequestType.Post,
            url="https://api.openai.com/v1/models/gpt-4",
            output_format=OutputFormat.Json,
            http_headers={"Authorization": "Bearer sk-xxx"}
        )

        created_version = ModelVersionFactory.create(external_hosting_config)

        assert created_version.hosting_location == ModelHostingLocation.External
        assert created_version.url == "https://api.openai.com/v1/models/gpt-4"

    def test_create_with_external_hosting_without_url_raises_error(self) -> None:
        missing_url_config = ModelVersionConfig(
            label="v1.0",
            short_description="External model without URL",
            long_description="Invalid external model configuration missing required URL",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with pytest.raises(ValueError, match="External models must define a url field"):
            ModelVersionFactory.create(missing_url_config)

    # --- HTTP Headers Handling ---

    def test_create_with_none_http_headers_sets_empty_dict(self) -> None:
        no_headers_config = ModelVersionConfig(
            label="v1.0",
            short_description="No headers model",
            long_description="Model without custom HTTP headers",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        created_version = ModelVersionFactory.create(no_headers_config)

        assert created_version.http_headers == {}

    def test_create_preserves_http_headers(self) -> None:
        authenticated_config = ModelVersionConfig(
            label="v1.0",
            short_description="Authenticated external model",
            long_description="External model requiring authentication headers",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.External,
            model_type=ModelType.Api,
            request_type=HttpRequestType.Post,
            url="https://api.example.com/predict",
            output_format=OutputFormat.Json,
            http_headers={"Authorization": "Bearer sk-xxx", "Content-Type": "application/json"}
        )

        created_version = ModelVersionFactory.create(authenticated_config)

        assert created_version.http_headers == {"Authorization": "Bearer sk-xxx", "Content-Type": "application/json"}

    def test_create_with_empty_http_headers(self) -> None:
        empty_headers_config = ModelVersionConfig(
            label="v1.0",
            short_description="Empty headers model",
            long_description="Model with explicitly empty HTTP headers",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers={}
        )

        created_version = ModelVersionFactory.create(empty_headers_config)

        assert created_version.http_headers == {}

    # --- Inputs Factory Integration ---

    def test_create_with_no_inputs(self, sample_version_config: ModelVersionConfig) -> None:
        with patch(SIGNATURE_FACTORY_PATH) as mock_factory:
            created_version = ModelVersionFactory.create(sample_version_config)

            assert created_version.inputs == []
            mock_factory.create.assert_not_called()

    def test_create_calls_signature_factory_for_input(self, sample_signature_config: SignatureConfig) -> None:
        single_input_config = ModelVersionConfig(
            label="v1.0",
            short_description="Single input model",
            long_description="Model with one audio input for speech recognition",
            assets=[],
            inputs=[sample_signature_config],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(SIGNATURE_FACTORY_PATH) as mock_factory:
            mock_input_signature = create_mock_signature(SignatureType.Input)
            mock_factory.create.return_value = mock_input_signature

            created_version = ModelVersionFactory.create(single_input_config)

            mock_factory.create.assert_called_once_with(sample_signature_config, SignatureType.Input)
            assert len(created_version.inputs) == 1
            assert created_version.inputs[0] == mock_input_signature

    def test_create_calls_signature_factory_for_each_input(self) -> None:
        text_input_config = SignatureConfig(
            display_title="Text Input",
            data_modality="text",
            data_domain="prompt",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )
        image_input_config = SignatureConfig(
            display_title="Image Input",
            data_modality="image",
            data_domain="photo",
            data_encoding="png",
            receive_format=ReceiveFormat.File,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )
        multi_input_config = ModelVersionConfig(
            label="v1.0",
            short_description="Multi-modal model",
            long_description="Vision-language model accepting both text and image inputs",
            assets=[],
            inputs=[text_input_config, image_input_config],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Transformer_hf,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(SIGNATURE_FACTORY_PATH) as mock_factory:
            mock_signatures = [
                create_mock_signature(SignatureType.Input),
                create_mock_signature(SignatureType.Input)
            ]
            mock_factory.create.side_effect = mock_signatures

            created_version = ModelVersionFactory.create(multi_input_config)

            assert mock_factory.create.call_count == 2
            assert len(created_version.inputs) == 2

    # --- Outputs Factory Integration ---

    def test_create_with_no_outputs(self, sample_version_config: ModelVersionConfig) -> None:
        created_version = ModelVersionFactory.create(sample_version_config)

        assert created_version.outputs == []

    def test_create_calls_signature_factory_for_output(self, sample_signature_config: SignatureConfig) -> None:
        single_output_config = ModelVersionConfig(
            label="v1.0",
            short_description="Single output model",
            long_description="Model with one text output for transcription results",
            assets=[],
            inputs=[],
            outputs=[sample_signature_config],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(SIGNATURE_FACTORY_PATH) as mock_factory:
            mock_output_signature = create_mock_signature(SignatureType.Output)
            mock_factory.create.return_value = mock_output_signature

            created_version = ModelVersionFactory.create(single_output_config)

            mock_factory.create.assert_called_once_with(sample_signature_config, SignatureType.Output)
            assert len(created_version.outputs) == 1

    # --- Assets Factory Integration ---

    def test_create_with_no_assets(self, sample_version_config: ModelVersionConfig) -> None:
        created_version = ModelVersionFactory.create(sample_version_config)

        assert created_version.assets == []

    def test_create_calls_asset_factory_for_asset(self, sample_asset_config: AssetConfig) -> None:
        single_asset_config = ModelVersionConfig(
            label="v1.0",
            short_description="Single asset model",
            long_description="Model with one PyTorch weights file",
            assets=[sample_asset_config],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(ASSET_FACTORY_PATH) as mock_factory:
            mock_asset = create_mock_asset(MODEL_WEIGHTS_ASSET)
            mock_factory.create.return_value = mock_asset

            created_version = ModelVersionFactory.create(single_asset_config)

            mock_factory.create.assert_called_once_with(sample_asset_config)
            assert len(created_version.assets) == 1
            assert created_version.assets[0] == mock_asset

    def test_create_calls_asset_factory_for_each_asset(self) -> None:
        weights_asset = AssetConfig(asset_name=MODEL_WEIGHTS_ASSET, data=b"weights data",source=InputSource.Primitive)
        config_asset = AssetConfig(asset_name="config.json", data=b"config data",source=InputSource.Primitive)
        multi_asset_config = ModelVersionConfig(
            label="v1.0",
            short_description="Multi-asset model",
            long_description="Model with weights and configuration files",
            assets=[weights_asset, config_asset],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(ASSET_FACTORY_PATH) as mock_factory:
            mock_assets = [
                create_mock_asset(MODEL_WEIGHTS_ASSET),
                create_mock_asset("config.json")
            ]
            mock_factory.create.side_effect = mock_assets

            created_version = ModelVersionFactory.create(multi_asset_config)

            assert mock_factory.create.call_count == 2
            assert len(created_version.assets) == 2

    # --- Mixed Inputs and Outputs ---

    def test_create_with_inputs_and_outputs(self, sample_signature_config: SignatureConfig) -> None:
        mixed_config = ModelVersionConfig(
            label="v1.0",
            short_description="Input-output model",
            long_description="Complete model with both input and output signatures",
            assets=[],
            inputs=[sample_signature_config],
            outputs=[sample_signature_config],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(SIGNATURE_FACTORY_PATH) as mock_factory:
            mock_input = create_mock_signature(SignatureType.Input)
            mock_output = create_mock_signature(SignatureType.Output)
            mock_factory.create.side_effect = [mock_input, mock_output]

            created_version = ModelVersionFactory.create(mixed_config)

            assert len(created_version.inputs) == 1
            assert len(created_version.outputs) == 1
            assert mock_factory.create.call_count == 2

    # --- Validation Errors ---

    def test_create_with_none_account_id_raises_validation_error(self) -> None:
        invalid_account_config = ModelVersionConfig(
            label="v1.0",
            short_description="Invalid account model",
            long_description="Model with null account for validation testing",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = None

            with pytest.raises(ValidationError):
                ModelVersionFactory.create(invalid_account_config)

    # --- Multiple Calls ---

    def test_create_multiple_versions_returns_independent_results(self) -> None:
        stable_version_config = ModelVersionConfig(
            label="v1.0-stable",
            short_description="Stable production release",
            long_description="Production-ready model with verified accuracy",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )
        experimental_version_config = ModelVersionConfig(
            label="v2.0-experimental",
            short_description="Experimental development release",
            long_description="Development version with new experimental features",
            assets=[],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Transformer_hf,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        stable_version = ModelVersionFactory.create(stable_version_config)
        experimental_version = ModelVersionFactory.create(experimental_version_config)

        assert stable_version.label == "v1.0-stable"
        assert experimental_version.label == "v2.0-experimental"

    # --- Error Propagation ---

    def test_create_propagates_signature_factory_error(self, sample_signature_config: SignatureConfig) -> None:
        error_config = ModelVersionConfig(
            label="v1.0",
            short_description="Signature error model",
            long_description="Model that causes signature factory error",
            assets=[],
            inputs=[sample_signature_config],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(SIGNATURE_FACTORY_PATH) as mock_factory:
            mock_factory.create.side_effect = ValueError("Invalid signature configuration")

            with pytest.raises(ValueError, match="Invalid signature configuration"):
                ModelVersionFactory.create(error_config)

    def test_create_propagates_asset_factory_error(self, sample_asset_config: AssetConfig) -> None:
        error_config = ModelVersionConfig(
            label="v1.0",
            short_description="Asset error model",
            long_description="Model that causes asset factory error",
            assets=[sample_asset_config],
            inputs=[],
            outputs=[],
            hosting_location=ModelHostingLocation.Internal,
            model_type=ModelType.Pytorch_jit,
            request_type=HttpRequestType.Post,
            url=None,
            output_format=OutputFormat.Json,
            http_headers=None
        )

        with patch(ASSET_FACTORY_PATH) as mock_factory:
            mock_factory.create.side_effect = ValueError("Invalid asset configuration")

            with pytest.raises(ValueError, match="Invalid asset configuration"):
                ModelVersionFactory.create(error_config)
