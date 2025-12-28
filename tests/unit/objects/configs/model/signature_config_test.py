import pytest

from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.configs.model.signature_config import SignatureConfig


@pytest.mark.unit
@pytest.mark.config
class TestSignatureConfig:
    def test_create_with_all_fields(self) -> None:
        display_title = "Audio Input"
        data_modality = "audio"
        data_domain = "speech"
        data_encoding = "wav"
        default_value = "default"

        parameter_config = ParameterConfig(
            parameter_name="audio_waveform",
            data_encoding=data_encoding,
        )
        signature_config = SignatureConfig(
            display_title=display_title,
            data_modality=data_modality,
            data_domain=data_domain,
            data_encoding=data_encoding,
            receive_format=ReceiveFormat.Primitive,
            parameters=[parameter_config],
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=default_value,
        )

        assert signature_config.display_title == display_title
        assert signature_config.data_modality == data_modality
        assert signature_config.data_domain == data_domain
        assert signature_config.data_encoding == data_encoding
        assert signature_config.receive_format == ReceiveFormat.Primitive
        assert len(signature_config.parameters) == 1
        assert signature_config.http_location == HttpLocation.Body
        assert signature_config.hidden is False
        assert signature_config.default_value == default_value

    def test_create_without_optional_fields(self) -> None:
        signature_config = SignatureConfig(
            display_title="Text Output",
            data_modality="text",
            data_domain="transcript",
            data_encoding="utf8",
            receive_format=ReceiveFormat.File,
            parameters=[],
        )

        assert signature_config.http_location is None
        assert signature_config.hidden is None
        assert signature_config.default_value is None

    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in SignatureConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {
            "display_title",
            "data_modality",
            "data_domain",
            "data_encoding",
            "receive_format",
            "parameters",
        }
