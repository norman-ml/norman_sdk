import pytest

from norman.objects.configs.model.parameter_config import ParameterConfig


@pytest.mark.unit
class TestParameterConfig:
    @pytest.mark.config
    def test_create_with_all_fields(self) -> None:
        parameter_name = "audio_waveform"
        data_encoding = "wav"

        parameter_config = ParameterConfig(
            parameter_name=parameter_name,
            data_encoding=data_encoding,
        )

        assert parameter_config.parameter_name == parameter_name
        assert parameter_config.data_encoding == data_encoding

    @pytest.mark.config
    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in ParameterConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {"parameter_name", "data_encoding"}
