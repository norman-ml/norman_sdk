import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.parameter_factory import ParameterFactory


class TestParameterFactory:

    @pytest.mark.parametrize("encoding,expected_modality", [
        ("wav", DataModality.Audio),
        ("mp3", DataModality.Audio),
        ("png", DataModality.Image),
        ("jpg", DataModality.Image),
        ("utf8", DataModality.Text),
        ("srt", DataModality.Text),
        ("h264", DataModality.Video),
        ("vp9", DataModality.Video),
    ])
    def test_create_resolves_encoding_to_modality(self, encoding: str, expected_modality: DataModality) -> None:
        config = ParameterConfig(parameter_name="test_param", data_encoding=encoding)

        created_parameter = ParameterFactory.create(config)

        assert created_parameter.data_modality == expected_modality
        assert created_parameter.data_encoding == encoding
        assert created_parameter.parameter_name == "test_param"

    def test_create_sets_default_id_values(self) -> None:
        config = ParameterConfig(parameter_name="audio_input", data_encoding="wav")

        created_parameter = ParameterFactory.create(config)

        assert created_parameter.id == "0"
        assert created_parameter.model_id == "0"
        assert created_parameter.version_id == "0"
        assert created_parameter.signature_id == "0"

    def test_create_handles_case_and_whitespace(self) -> None:
        config = ParameterConfig(parameter_name="audio_input", data_encoding="  WAV  ")

        created_parameter = ParameterFactory.create(config)

        assert created_parameter.data_modality == DataModality.Audio

    def test_create_with_unknown_encoding_raises_value_error(self) -> None:
        config = ParameterConfig(parameter_name="invalid_param", data_encoding="unknown_format")

        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterFactory.create(config)
