import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.parameter_factory import ParameterFactory
from tests.constants import DEFAULT_ID, DEFAULT_MODEL_ID, DEFAULT_SIGNATURE_ID, DEFAULT_VERSION_ID


@pytest.mark.unit
@pytest.mark.factory
class TestParameterFactory:
    def test_create_returns_model_param_with_resolved_modality(self) -> None:
        parameter_name = "audio_waveform"
        data_encoding = "wav"

        parameter_config = ParameterConfig(
            parameter_name=parameter_name,
            data_encoding=data_encoding,
        )

        model_param = ParameterFactory.create(parameter_config)

        assert model_param.parameter_name == parameter_name
        assert model_param.data_encoding == data_encoding
        assert model_param.data_modality == DataModality.Audio

    def test_create_sets_default_id_values(self) -> None:
        parameter_config = ParameterConfig(
            parameter_name="audio_input",
            data_encoding="wav",
        )

        model_param = ParameterFactory.create(parameter_config)

        assert model_param.id == DEFAULT_ID
        assert model_param.model_id == DEFAULT_MODEL_ID
        assert model_param.version_id == DEFAULT_VERSION_ID
        assert model_param.signature_id == DEFAULT_SIGNATURE_ID

    def test_create_handles_case_and_whitespace(self) -> None:
        parameter_config = ParameterConfig(
            parameter_name="audio_input",
            data_encoding="  WAV  ",
        )

        model_param = ParameterFactory.create(parameter_config)

        assert model_param.data_modality == DataModality.Audio

    def test_create_with_unknown_encoding_raises_value_error(self) -> None:
        parameter_config = ParameterConfig(
            parameter_name="invalid_param",
            data_encoding="unknown_format",
        )

        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterFactory.create(parameter_config)
