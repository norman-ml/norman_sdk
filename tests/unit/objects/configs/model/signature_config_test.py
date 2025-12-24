from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.configs.model.signature_config import SignatureConfig


class TestSignatureConfig:

    def test_create_with_all_fields(self) -> None:
        parameter = ParameterConfig(parameter_name="audio_waveform", data_encoding="wav")
        config = SignatureConfig(display_title="Audio Input", data_modality="audio", data_domain="speech", data_encoding="wav", receive_format=ReceiveFormat.Primitive, parameters=[parameter], http_location=HttpLocation.Body, hidden=False, default_value="default")

        assert config.display_title == "Audio Input"
        assert config.data_modality == "audio"
        assert config.data_domain == "speech"
        assert config.data_encoding == "wav"
        assert config.receive_format == ReceiveFormat.Primitive
        assert len(config.parameters) == 1
        assert config.http_location == HttpLocation.Body
        assert config.hidden is False
        assert config.default_value == "default"

    def test_create_without_optional_fields(self) -> None:
        config = SignatureConfig(display_title="Text Output", data_modality="text", data_domain="transcript", data_encoding="utf8", receive_format=ReceiveFormat.File, parameters=[])

        assert config.http_location is None
        assert config.hidden is None
        assert config.default_value is None

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in SignatureConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"display_title", "data_modality", "data_domain", "data_encoding", "receive_format", "parameters"}
