import pytest
from unittest.mock import patch

from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.parameters.data_modality import DataModality
from norman_objects.shared.parameters.model_param import ModelParam

from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.signature_factory import SignatureFactory


SIGNATURE_MODALITY_RESOLVER_PATH = "norman.objects.factories.signature_factory.SignatureModalityResolver"
PARAMETER_FACTORY_PATH = "norman.objects.factories.signature_factory.ParameterFactory"
AUDIO_INPUT = "Audio Input"

@pytest.fixture
def mock_signature_modality_resolver():
    with patch(SIGNATURE_MODALITY_RESOLVER_PATH) as mock:
        mock.resolve.return_value = DataModality.Audio
        yield mock


@pytest.fixture
def mock_parameter_factory():
    with patch(PARAMETER_FACTORY_PATH) as mock:
        yield mock


def create_mock_param(name: str) -> ModelParam:
    return ModelParam(id="0", model_id="0", version_id="0", signature_id="0", data_modality=DataModality.Audio, data_encoding="wav", parameter_name=name)


@pytest.mark.usefixtures("mock_signature_modality_resolver", "mock_parameter_factory")
class TestSignatureFactory:

    def test_create_returns_model_signature_with_defaults(self) -> None:
        config = SignatureConfig(display_title=AUDIO_INPUT, data_modality="audio", data_domain="speech", data_encoding="wav", receive_format=ReceiveFormat.Primitive, parameters=[])

        created_signature = SignatureFactory.create(config, SignatureType.Input)

        assert isinstance(created_signature, ModelSignature)
        assert created_signature.signature_type == SignatureType.Input
        assert created_signature.display_title == AUDIO_INPUT
        assert created_signature.data_domain == "speech"
        assert created_signature.data_encoding == "wav"
        assert created_signature.receive_format == ReceiveFormat.Primitive
        assert created_signature.http_location == HttpLocation.Body
        assert created_signature.hidden is False
        assert created_signature.default_value is None
        assert created_signature.transforms == []
        assert created_signature.signature_args == {}
        assert created_signature.id == "0"
        assert created_signature.model_id == "0"
        assert created_signature.version_id == "0"

    def test_create_resolves_data_modality_from_encoding(self) -> None:
        config = SignatureConfig(display_title="Video Input", data_modality="video", data_domain="stream", data_encoding="mp4", receive_format=ReceiveFormat.File, parameters=[])

        with patch(SIGNATURE_MODALITY_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Video

            created_signature = SignatureFactory.create(config, SignatureType.Input)

            mock_resolver.resolve.assert_called_once_with("mp4")
            assert created_signature.data_modality == DataModality.Video

    def test_create_preserves_explicit_http_location(self) -> None:
        config = SignatureConfig(display_title="Query Param", data_modality="text", data_domain="query", data_encoding="utf8", receive_format=ReceiveFormat.Primitive, http_location=HttpLocation.Query, parameters=[])

        created_signature = SignatureFactory.create(config, SignatureType.Input)

        assert created_signature.http_location == HttpLocation.Query

    def test_create_preserves_hidden_and_default_value(self) -> None:
        config = SignatureConfig(display_title="Hidden Input", data_modality="text", data_domain="config", data_encoding="utf8", receive_format=ReceiveFormat.Primitive, hidden=True, default_value="en-US", parameters=[])

        created_signature = SignatureFactory.create(config, SignatureType.Input)

        assert created_signature.hidden is True
        assert created_signature.default_value == "en-US"

    def test_create_calls_parameter_factory_for_parameters(self) -> None:
        param_config = ParameterConfig(parameter_name="audio_waveform", data_encoding="wav")
        config = SignatureConfig(display_title=AUDIO_INPUT, data_modality="audio", data_domain="speech", data_encoding="wav", receive_format=ReceiveFormat.Primitive, parameters=[param_config])

        with patch(PARAMETER_FACTORY_PATH) as mock_factory:
            mock_param = create_mock_param("audio_waveform")
            mock_factory.create.return_value = mock_param

            created_signature = SignatureFactory.create(config, SignatureType.Input)

            mock_factory.create.assert_called_once_with(param_config)
            assert len(created_signature.parameters) == 1

    def test_create_with_output_signature_type(self) -> None:
        config = SignatureConfig(display_title="Text Output", data_modality="text", data_domain="transcript", data_encoding="utf8", receive_format=ReceiveFormat.Primitive, parameters=[])

        created_signature = SignatureFactory.create(config, SignatureType.Output)

        assert created_signature.signature_type == SignatureType.Output
        