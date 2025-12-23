import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.parameter_factory import ParameterFactory


class TestParameterFactoryCreate:
    @pytest.mark.parametrize("encoding", ["wav", "mp3", "flac", "aac", "ac3", "opus", "vorbis"])
    def test_create_with_audio_encoding_returns_audio_modality(self, encoding):
        config = ParameterConfig(parameter_name="audio_input", data_encoding=encoding)

        result = ParameterFactory.create(config)

        assert result.data_modality == DataModality.Audio
        assert result.data_encoding == encoding
        assert result.parameter_name == "audio_input"

    @pytest.mark.parametrize("encoding", ["jpg", "jpeg", "png", "webp"])
    def test_create_with_image_encoding_returns_image_modality(self, encoding):
        config = ParameterConfig(parameter_name="image_input", data_encoding=encoding)

        result = ParameterFactory.create(config)

        assert result.data_modality == DataModality.Image
        assert result.data_encoding == encoding
        assert result.parameter_name == "image_input"

    @pytest.mark.parametrize("encoding", ["utf8", "utf16", "srt", "vtt", "ass", "mp4_text"])
    def test_create_with_text_encoding_returns_text_modality(self, encoding):
        config = ParameterConfig(parameter_name="text_input", data_encoding=encoding)

        result = ParameterFactory.create(config)

        assert result.data_modality == DataModality.Text
        assert result.data_encoding == encoding
        assert result.parameter_name == "text_input"

    @pytest.mark.parametrize("encoding", ["h264", "h265", "av1", "vp8", "vp9"])
    def test_create_with_video_encoding_returns_video_modality(self, encoding):
        config = ParameterConfig(parameter_name="video_input", data_encoding=encoding)
        result = ParameterFactory.create(config)

        assert result.data_modality == DataModality.Video
        assert result.data_encoding == encoding
        assert result.parameter_name == "video_input"


    @pytest.mark.parametrize("encoding", ["WAV", "Wav", "wAv", "  wav  ", "  WAV  "])
    def test_create_handles_case_and_whitespace_in_encoding(self, encoding):
        config = ParameterConfig(parameter_name="audio_input", data_encoding=encoding)
        result = ParameterFactory.create(config)

        assert result.data_modality == DataModality.Audio

    def test_create_sets_default_id_values(self):
        config = ParameterConfig(parameter_name="test_param", data_encoding="png")
        result = ParameterFactory.create(config)

        assert result.id == "0"
        assert result.model_id == "0"
        assert result.version_id == "0"
        assert result.signature_id == "0"


    def test_create_with_unknown_encoding_raises_value_error(self):
        config = ParameterConfig(parameter_name="test_param", data_encoding="unknown_format")

        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterFactory.create(config)

    def test_create_with_empty_encoding_raises_value_error(self):
        config = ParameterConfig(parameter_name="test_param", data_encoding="")

        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterFactory.create(config)

    def test_create_with_whitespace_only_encoding_raises_value_error(self):
        config = ParameterConfig(parameter_name="test_param", data_encoding="   ")

        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterFactory.create(config)

    def test_create_preserves_exact_parameter_name(self):
        config = ParameterConfig(
            parameter_name="My_Special_Parameter_123",
            data_encoding="wav"
        )

        result = ParameterFactory.create(config)
        assert result.parameter_name == "My_Special_Parameter_123"

    def test_create_preserves_exact_data_encoding(self):
        config = ParameterConfig(parameter_name="test", data_encoding="WAV")
        result = ParameterFactory.create(config)
        assert result.data_encoding == "WAV"

