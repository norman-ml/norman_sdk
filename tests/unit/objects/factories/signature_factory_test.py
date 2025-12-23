import pytest
from unittest.mock import patch, MagicMock

from pydantic import ValidationError

from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.parameters.data_modality import DataModality
from norman_objects.shared.parameters.model_param import ModelParam

from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.signature_factory import SignatureFactory


def create_signature_config(**overrides):
    """Helper to create a valid SignatureConfig"""
    defaults = {
        "display_title": "Test Signature",
        "data_modality": "audio",
        "data_domain": "speech",
        "data_encoding": "wav",
        "receive_format": ReceiveFormat.Primitive,
        "parameters": []
    }
    defaults.update(overrides)
    return SignatureConfig(**defaults)


def create_mock_param(name="test_param"):
    """Helper to create a real ModelParam for Pydantic compatibility"""
    return ModelParam(
        id="0",
        model_id="0",
        version_id="0",
        signature_id="0",
        data_modality=DataModality.Audio,
        data_encoding="wav",
        parameter_name=name
    )


class TestSignatureFactoryCreate:
    """Tests for SignatureFactory.create() method"""

    # --- Success Cases ---

    def test_create_returns_model_signature_instance(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert isinstance(result, ModelSignature)

    def test_create_sets_signature_type_input(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.signature_type == SignatureType.Input

    def test_create_sets_signature_type_output(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Output)

        assert result.signature_type == SignatureType.Output

    def test_create_sets_display_title_from_config(self):
        config = create_signature_config(display_title="My Input")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.display_title == "My Input"

    def test_create_sets_data_domain_from_config(self):
        config = create_signature_config(data_domain="music")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.data_domain == "music"

    def test_create_sets_data_encoding_from_config(self):
        config = create_signature_config(data_encoding="mp3")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.data_encoding == "mp3"

    def test_create_sets_receive_format_from_config(self):
        config = create_signature_config(receive_format=ReceiveFormat.File)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.receive_format == ReceiveFormat.File

    # --- Data Modality Resolution ---

    def test_create_calls_resolver_with_data_encoding(self):
        config = create_signature_config(data_encoding="wav")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            SignatureFactory.create(config, SignatureType.Input)

            mock_resolver.resolve.assert_called_once_with("wav")

    def test_create_sets_data_modality_from_resolver(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Video
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.data_modality == DataModality.Video

    # --- HTTP Location Handling ---

    def test_create_with_none_http_location_defaults_to_body(self):
        config = create_signature_config(http_location=None)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.http_location == HttpLocation.Body

    def test_create_preserves_explicit_http_location_path(self):
        config = create_signature_config(http_location=HttpLocation.Path)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.http_location == HttpLocation.Path

    def test_create_preserves_explicit_http_location_query(self):
        config = create_signature_config(http_location=HttpLocation.Query)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.http_location == HttpLocation.Query

    def test_create_preserves_explicit_http_location_body(self):
        config = create_signature_config(http_location=HttpLocation.Body)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.http_location == HttpLocation.Body

    # --- Hidden Handling ---

    def test_create_with_none_hidden_defaults_to_false(self):
        config = create_signature_config(hidden=None)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.hidden is False

    def test_create_preserves_hidden_true(self):
        config = create_signature_config(hidden=True)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.hidden is True

    def test_create_preserves_hidden_false(self):
        config = create_signature_config(hidden=False)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.hidden is False

    # --- Default Value Handling ---

    def test_create_with_none_default_value(self):
        config = create_signature_config(default_value=None)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.default_value is None

    def test_create_preserves_default_value(self):
        config = create_signature_config(default_value="my default")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.default_value == "my default"

    def test_create_preserves_empty_default_value(self):
        config = create_signature_config(default_value="")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.default_value == ""

    # --- Parameters Handling ---

    def test_create_with_no_parameters(self):
        config = create_signature_config(parameters=[])

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver, \
             patch('norman.objects.factories.signature_factory.ParameterFactory') as mock_param_factory:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.parameters == []
        mock_param_factory.create.assert_not_called()

    def test_create_with_single_parameter(self):
        param_config = ParameterConfig(parameter_name="audio_input", data_encoding="wav")
        config = create_signature_config(parameters=[param_config])

        mock_param = create_mock_param("audio_input")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver, \
             patch('norman.objects.factories.signature_factory.ParameterFactory') as mock_param_factory:
            mock_resolver.resolve.return_value = DataModality.Audio
            mock_param_factory.create.return_value = mock_param

            result = SignatureFactory.create(config, SignatureType.Input)

        assert len(result.parameters) == 1
        assert result.parameters[0] == mock_param
        mock_param_factory.create.assert_called_once_with(param_config)

    def test_create_with_multiple_parameters(self):
        param_configs = [
            ParameterConfig(parameter_name="param1", data_encoding="wav"),
            ParameterConfig(parameter_name="param2", data_encoding="mp3")
        ]
        config = create_signature_config(parameters=param_configs)

        mock_params = [
            create_mock_param("param1"),
            create_mock_param("param2")
        ]

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver, \
             patch('norman.objects.factories.signature_factory.ParameterFactory') as mock_param_factory:
            mock_resolver.resolve.return_value = DataModality.Audio
            mock_param_factory.create.side_effect = mock_params

            result = SignatureFactory.create(config, SignatureType.Input)

        assert len(result.parameters) == 2
        assert mock_param_factory.create.call_count == 2

    def test_create_calls_parameter_factory_for_each_parameter(self):
        param_configs = [
            ParameterConfig(parameter_name="param1", data_encoding="wav"),
            ParameterConfig(parameter_name="param2", data_encoding="mp3")
        ]
        config = create_signature_config(parameters=param_configs)

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver, \
             patch('norman.objects.factories.signature_factory.ParameterFactory') as mock_param_factory:
            mock_resolver.resolve.return_value = DataModality.Audio
            mock_param_factory.create.side_effect = [
                create_mock_param("param1"),
                create_mock_param("param2")
            ]

            SignatureFactory.create(config, SignatureType.Input)

            mock_param_factory.create.assert_any_call(param_configs[0])
            mock_param_factory.create.assert_any_call(param_configs[1])

    # --- Fixed/Empty Values ---

    def test_create_sets_transforms_to_empty_list(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.transforms == []

    def test_create_sets_signature_args_to_empty_dict(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.signature_args == {}

    # --- Default ID Values ---

    def test_create_sets_default_id(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.id == "0"

    def test_create_sets_default_model_id(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.model_id == "0"

    def test_create_sets_default_version_id(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.version_id == "0"

    # --- Error Propagation ---

    def test_create_propagates_resolver_error(self):
        config = create_signature_config(data_encoding="unknown")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.side_effect = ValueError("Unknown signature encoding")

            with pytest.raises(ValueError, match="Unknown signature encoding"):
                SignatureFactory.create(config, SignatureType.Input)

    def test_create_propagates_parameter_factory_error(self):
        param_config = ParameterConfig(parameter_name="test", data_encoding="wav")
        config = create_signature_config(parameters=[param_config])

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver, \
             patch('norman.objects.factories.signature_factory.ParameterFactory') as mock_param_factory:
            mock_resolver.resolve.return_value = DataModality.Audio
            mock_param_factory.create.side_effect = ValueError("Parameter factory error")

            with pytest.raises(ValueError, match="Parameter factory error"):
                SignatureFactory.create(config, SignatureType.Input)

    # --- Multiple Calls ---

    def test_create_multiple_signatures_with_different_types(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio

            result1 = SignatureFactory.create(config, SignatureType.Input)
            result2 = SignatureFactory.create(config, SignatureType.Output)

        assert result1.signature_type == SignatureType.Input
        assert result2.signature_type == SignatureType.Output


class TestSignatureFactoryResolverIntegration:
    """Tests verifying SignatureFactory's integration with SignatureModalityResolver"""

    def test_resolver_called_with_correct_encoding(self):
        config = create_signature_config(data_encoding="mp4")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Video
            SignatureFactory.create(config, SignatureType.Input)

            mock_resolver.resolve.assert_called_once_with("mp4")

    def test_resolver_return_value_used_for_modality(self):
        config = create_signature_config()

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Image
            result = SignatureFactory.create(config, SignatureType.Input)

        assert result.data_modality == DataModality.Image

    def test_different_encodings_resolve_to_different_modalities(self):
        audio_config = create_signature_config(data_encoding="wav")
        video_config = create_signature_config(data_encoding="mp4")

        with patch('norman.objects.factories.signature_factory.SignatureModalityResolver') as mock_resolver:
            mock_resolver.resolve.side_effect = [DataModality.Audio, DataModality.Video]

            audio_result = SignatureFactory.create(audio_config, SignatureType.Input)
            video_result = SignatureFactory.create(video_config, SignatureType.Input)

        assert audio_result.data_modality == DataModality.Audio
        assert video_result.data_modality == DataModality.Video