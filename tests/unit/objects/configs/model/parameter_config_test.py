from norman.objects.configs.model.parameter_config import ParameterConfig


class TestParameterConfig:

    def test_create_with_all_fields(self) -> None:
        config = ParameterConfig(parameter_name="audio_waveform", data_encoding="wav")

        assert config.parameter_name == "audio_waveform"
        assert config.data_encoding == "wav"

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in ParameterConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"parameter_name", "data_encoding"}
