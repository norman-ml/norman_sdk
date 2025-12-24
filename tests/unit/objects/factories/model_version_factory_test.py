import pytest
from unittest.mock import patch, PropertyMock, MagicMock

from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.parameters.data_modality import DataModality
from pydantic import ValidationError

from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model_build_status import ModelBuildStatus
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.model_version import ModelVersion
from norman_objects.shared.models.output_format import OutputFormat
from norman_objects.shared.models.model_asset import ModelAsset

from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.model_version_factory import ModelVersionFactory
from norman.managers.authentication_manager import AuthenticationManager


def create_minimal_version_config(**overrides):
    """Helper to create a minimal valid ModelVersionConfig"""
    defaults = {
        "label": "v1.0",
        "short_description": "Test model",
        "long_description": "A test model for unit testing",
        "assets": [],
        "inputs": [],
        "outputs": []
    }
    defaults.update(overrides)
    return ModelVersionConfig(**defaults)



def create_mock_signature(signature_type=SignatureType.Input):
    """Helper to create a real ModelSignature for Pydantic compatibility"""
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
        display_title="Test Signature",
        default_value=None,
        parameters=[],
        transforms=[],
        signature_args={}
    )

def create_mock_asset():
    """Helper to create a real ModelAsset for Pydantic compatibility"""
    return ModelAsset(
        id="0",
        account_id="account-123",
        model_id="0",
        version_id="0",
        asset_name="mock-asset"
    )


def create_signature_config():
    """Helper to create a valid SignatureConfig"""
    return SignatureConfig(
        display_title="Test Input",
        data_modality="audio",
        data_domain="speech",
        data_encoding="wav",
        receive_format="Primitive",  # Fixed: use valid enum value
        parameters=[]
    )


class TestModelVersionFactoryCreate:
    """Tests for ModelVersionFactory.create() method"""

    # --- Success Cases ---

    def test_create_returns_model_version_instance(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert isinstance(result, ModelVersion)

    def test_create_sets_label_from_config(self):
        config = create_minimal_version_config(label="v2.0-beta")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.label == "v2.0-beta"

    def test_create_sets_short_description_from_config(self):
        config = create_minimal_version_config(short_description="A short desc")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.short_description == "A short desc"

    def test_create_sets_long_description_from_config(self):
        config = create_minimal_version_config(long_description="A very long description")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.long_description == "A very long description"

    def test_create_sets_account_id_from_authentication_manager(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-456"
            result = ModelVersionFactory.create(config)

        assert result.account_id == "account-456"

    # --- Fixed Values ---

    def test_create_sets_build_status_to_in_progress(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.build_status == ModelBuildStatus.InProgress

    def test_create_sets_active_to_true(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.active is True

    # --- Default Values ---

    def test_create_sets_default_id(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.id == "0"

    def test_create_sets_default_model_id(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.model_id == "0"

    # --- Request Type Handling ---

    def test_create_with_none_request_type_defaults_to_post(self):
        config = create_minimal_version_config(request_type=None)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.request_type == HttpRequestType.Post

    def test_create_preserves_explicit_request_type_get(self):
        config = create_minimal_version_config(request_type=HttpRequestType.Get)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.request_type == HttpRequestType.Get

    def test_create_preserves_explicit_request_type_put(self):
        config = create_minimal_version_config(request_type=HttpRequestType.Put)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.request_type == HttpRequestType.Put

    # --- Model Type Handling ---

    def test_create_with_none_model_type_defaults_to_pytorch_jit(self):
        config = create_minimal_version_config(model_type=None)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.model_type == ModelType.Pytorch_jit

    def test_create_preserves_explicit_model_type_api(self):
        config = create_minimal_version_config(model_type=ModelType.Api)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.model_type == ModelType.Api

    def test_create_preserves_explicit_model_type_transformer(self):
        config = create_minimal_version_config(model_type=ModelType.Transformer_hf)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.model_type == ModelType.Transformer_hf

    # --- Output Format Handling ---

    def test_create_with_none_output_format_defaults_to_json(self):
        config = create_minimal_version_config(output_format=None)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.output_format == OutputFormat.Json

    def test_create_preserves_explicit_output_format_binary(self):
        config = create_minimal_version_config(output_format=OutputFormat.Binary)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.output_format == OutputFormat.Binary

    def test_create_preserves_explicit_output_format_text(self):
        config = create_minimal_version_config(output_format=OutputFormat.Text)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.output_format == OutputFormat.Text

    # --- Hosting Location Handling ---

    def test_create_with_none_hosting_location_defaults_to_internal(self):
        config = create_minimal_version_config(hosting_location=None)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.hosting_location == ModelHostingLocation.Internal

    def test_create_with_internal_hosting_sets_empty_url(self):
        config = create_minimal_version_config(hosting_location=ModelHostingLocation.Internal)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.url == ""

    def test_create_with_external_hosting_and_url(self):
        config = create_minimal_version_config(
            hosting_location=ModelHostingLocation.External,
            url="https://api.example.com/model"
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.hosting_location == ModelHostingLocation.External
        assert result.url == "https://api.example.com/model"

    def test_create_with_external_hosting_without_url_raises_error(self):
        config = create_minimal_version_config(
            hosting_location=ModelHostingLocation.External,
            url=None
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"

            with pytest.raises(ValueError, match="External models must define a url field"):
                ModelVersionFactory.create(config)

    # --- HTTP Headers Handling ---

    def test_create_with_none_http_headers_sets_empty_dict(self):
        config = create_minimal_version_config(http_headers=None)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.http_headers == {}

    def test_create_preserves_http_headers(self):
        headers = {"Authorization": "Bearer token", "Content-Type": "application/json"}
        config = create_minimal_version_config(http_headers=headers)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.http_headers == headers

    def test_create_with_empty_http_headers(self):
        config = create_minimal_version_config(http_headers={})

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = ModelVersionFactory.create(config)

        assert result.http_headers == {}

    # --- Inputs Handling ---

    def test_create_with_no_inputs(self):
        config = create_minimal_version_config(inputs=[])

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"

            result = ModelVersionFactory.create(config)

        assert result.inputs == []
        mock_sig_factory.create.assert_not_called()

    def test_create_with_single_input(self):
        sig_config = create_signature_config()
        config = create_minimal_version_config(inputs=[sig_config])

        mock_signature = create_mock_signature(SignatureType.Input)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"
            mock_sig_factory.create.return_value = mock_signature

            result = ModelVersionFactory.create(config)

        assert len(result.inputs) == 1
        assert result.inputs[0] == mock_signature
        mock_sig_factory.create.assert_called_once_with(sig_config, SignatureType.Input)

    def test_create_with_multiple_inputs(self):
        sig_configs = [create_signature_config(), create_signature_config()]
        config = create_minimal_version_config(inputs=sig_configs)

        mock_signatures = [create_mock_signature(SignatureType.Input) for _ in range(2)]

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"
            mock_sig_factory.create.side_effect = mock_signatures

            result = ModelVersionFactory.create(config)

        assert len(result.inputs) == 2

    def test_create_calls_signature_factory_with_input_type(self):
        sig_config = create_signature_config()
        config = create_minimal_version_config(inputs=[sig_config])

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"
            mock_sig_factory.create.return_value = create_mock_signature()

            ModelVersionFactory.create(config)

            mock_sig_factory.create.assert_called_with(sig_config, SignatureType.Input)

    # --- Outputs Handling ---

    def test_create_with_no_outputs(self):
        config = create_minimal_version_config(outputs=[])

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"

            result = ModelVersionFactory.create(config)

        assert result.outputs == []

    def test_create_with_single_output(self):
        sig_config = create_signature_config()
        config = create_minimal_version_config(outputs=[sig_config])

        mock_signature = create_mock_signature(SignatureType.Output)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"
            mock_sig_factory.create.return_value = mock_signature

            result = ModelVersionFactory.create(config)

        assert len(result.outputs) == 1
        mock_sig_factory.create.assert_called_once_with(sig_config, SignatureType.Output)

    def test_create_calls_signature_factory_with_output_type(self):
        sig_config = create_signature_config()
        config = create_minimal_version_config(outputs=[sig_config])

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"
            mock_sig_factory.create.return_value = create_mock_signature()

            ModelVersionFactory.create(config)

            mock_sig_factory.create.assert_called_with(sig_config, SignatureType.Output)

    # --- Assets Handling ---

    def test_create_with_no_assets(self):
        config = create_minimal_version_config(assets=[])

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"

            result = ModelVersionFactory.create(config)

        assert result.assets == []

    def test_create_with_single_asset(self):
        asset_config = AssetConfig(asset_name="model.pt", data=b"binary data")
        config = create_minimal_version_config(assets=[asset_config])

        mock_asset = create_mock_asset()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.AssetFactory') as mock_asset_factory:
            mock_account_id.return_value = "account-123"
            mock_asset_factory.create.return_value = mock_asset

            result = ModelVersionFactory.create(config)

        assert len(result.assets) == 1
        assert result.assets[0] == mock_asset
        mock_asset_factory.create.assert_called_once_with(asset_config)

    def test_create_with_multiple_assets(self):
        asset_configs = [
            AssetConfig(asset_name="model.pt", data=b"data1"),
            AssetConfig(asset_name="config.json", data=b"data2")
        ]
        config = create_minimal_version_config(assets=asset_configs)

        mock_assets = [create_mock_asset(), create_mock_asset()]

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.AssetFactory') as mock_asset_factory:
            mock_account_id.return_value = "account-123"
            mock_asset_factory.create.side_effect = mock_assets

            result = ModelVersionFactory.create(config)

        assert len(result.assets) == 2
        assert mock_asset_factory.create.call_count == 2

    # --- Mixed Inputs and Outputs ---

    def test_create_with_inputs_and_outputs(self):
        input_config = create_signature_config()
        output_config = create_signature_config()
        config = create_minimal_version_config(inputs=[input_config], outputs=[output_config])

        mock_input = create_mock_signature(SignatureType.Input)
        mock_output = create_mock_signature(SignatureType.Output)

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"
            mock_sig_factory.create.side_effect = [mock_input, mock_output]

            result = ModelVersionFactory.create(config)

        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert mock_sig_factory.create.call_count == 2

    # --- Account ID Edge Cases ---

    def test_create_with_none_account_id_raises_validation_error(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = None

            with pytest.raises(ValidationError):
                ModelVersionFactory.create(config)

    # --- Multiple Calls ---

    def test_create_multiple_versions_with_different_labels(self):
        config1 = create_minimal_version_config(label="v1.0")
        config2 = create_minimal_version_config(label="v2.0")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"

            result1 = ModelVersionFactory.create(config1)
            result2 = ModelVersionFactory.create(config2)

        assert result1.label == "v1.0"
        assert result2.label == "v2.0"


class TestModelVersionFactoryIntegration:
    """Tests verifying factory integration with dependencies"""

    def test_create_reads_account_id_property(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"

            ModelVersionFactory.create(config)

            mock_account_id.assert_called()

    def test_create_uses_current_account_id_at_call_time(self):
        config = create_minimal_version_config()

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "first-account"
            result1 = ModelVersionFactory.create(config)

            mock_account_id.return_value = "second-account"
            result2 = ModelVersionFactory.create(config)

        assert result1.account_id == "first-account"
        assert result2.account_id == "second-account"

    def test_signature_factory_error_propagates(self):
        sig_config = create_signature_config()
        config = create_minimal_version_config(inputs=[sig_config])

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.SignatureFactory') as mock_sig_factory:
            mock_account_id.return_value = "account-123"
            mock_sig_factory.create.side_effect = ValueError("Signature factory error")

            with pytest.raises(ValueError, match="Signature factory error"):
                ModelVersionFactory.create(config)

    def test_asset_factory_error_propagates(self):
        asset_config = AssetConfig(asset_name="model.pt", data=b"data")
        config = create_minimal_version_config(assets=[asset_config])

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_version_factory.AssetFactory') as mock_asset_factory:
            mock_account_id.return_value = "account-123"
            mock_asset_factory.create.side_effect = ValueError("Asset factory error")

            with pytest.raises(ValueError, match="Asset factory error"):
                ModelVersionFactory.create(config)