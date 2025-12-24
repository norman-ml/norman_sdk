import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.parameter_factory import ParameterFactory

UNKNOWN_PARAMETER = "Unknown parameter encoding"

@pytest.fixture
def sample_parameter_config() -> ParameterConfig:
    return ParameterConfig(
        parameter_name="audio_waveform",
        data_encoding="wav"
    )

class TestParameterFactory:

    # --- Audio Encodings ---

    @pytest.mark.parametrize("encoding", ["wav", "mp3", "flac", "aac", "ac3", "opus", "vorbis"])
    def test_create_with_audio_encoding_returns_audio_modality(self, encoding: str) -> None:
        audio_config = ParameterConfig(parameter_name="speech_input", data_encoding=encoding)

        created_parameter = ParameterFactory.create(audio_config)

        assert created_parameter.data_modality == DataModality.Audio
        assert created_parameter.data_encoding == encoding
        assert created_parameter.parameter_name == "speech_input"

    # --- Image Encodings ---

    @pytest.mark.parametrize("encoding", ["jpg", "jpeg", "png", "webp"])
    def test_create_with_image_encoding_returns_image_modality(self, encoding: str) -> None:
        image_config = ParameterConfig(parameter_name="photo_input", data_encoding=encoding)

        created_parameter = ParameterFactory.create(image_config)

        assert created_parameter.data_modality == DataModality.Image
        assert created_parameter.data_encoding == encoding
        assert created_parameter.parameter_name == "photo_input"

    # --- Text Encodings ---

    @pytest.mark.parametrize("encoding", ["utf8", "utf16", "srt", "vtt", "ass", "mp4_text"])
    def test_create_with_text_encoding_returns_text_modality(self, encoding: str) -> None:
        text_config = ParameterConfig(parameter_name="transcript_input", data_encoding=encoding)

        created_parameter = ParameterFactory.create(text_config)

        assert created_parameter.data_modality == DataModality.Text
        assert created_parameter.data_encoding == encoding
        assert created_parameter.parameter_name == "transcript_input"

    # --- Video Encodings ---

    @pytest.mark.parametrize("encoding", ["h264", "h265", "av1", "vp8", "vp9"])
    def test_create_with_video_encoding_returns_video_modality(self, encoding: str) -> None:
        video_config = ParameterConfig(parameter_name="video_stream", data_encoding=encoding)

        created_parameter = ParameterFactory.create(video_config)

        assert created_parameter.data_modality == DataModality.Video
        assert created_parameter.data_encoding == encoding
        assert created_parameter.parameter_name == "video_stream"

    # --- Case and Whitespace Handling ---

    @pytest.mark.parametrize("encoding", ["WAV", "Wav", "wAv", "  wav  ", "  WAV  "])
    def test_create_handles_case_and_whitespace_in_encoding(self, encoding: str) -> None:
        case_insensitive_config = ParameterConfig(parameter_name="audio_input", data_encoding=encoding)

        created_parameter = ParameterFactory.create(case_insensitive_config)

        assert created_parameter.data_modality == DataModality.Audio

    # --- Default Values ---

    def test_create_sets_default_id_values(self) -> None:
        default_id_config = ParameterConfig(parameter_name="thumbnail", data_encoding="png")

        created_parameter = ParameterFactory.create(default_id_config)

        assert created_parameter.id == "0"
        assert created_parameter.model_id == "0"
        assert created_parameter.version_id == "0"
        assert created_parameter.signature_id == "0"

    # --- Validation Errors ---

    def test_create_with_unknown_encoding_raises_value_error(self) -> None:
        unknown_encoding_config = ParameterConfig(parameter_name="invalid_param", data_encoding="unknown_format")

        with pytest.raises(ValueError, match=UNKNOWN_PARAMETER):
            ParameterFactory.create(unknown_encoding_config)

    def test_create_with_empty_encoding_raises_value_error(self) -> None:
        empty_encoding_config = ParameterConfig(parameter_name="empty_param", data_encoding="")

        with pytest.raises(ValueError, match=UNKNOWN_PARAMETER):
            ParameterFactory.create(empty_encoding_config)

    def test_create_with_whitespace_only_encoding_raises_value_error(self) -> None:
        whitespace_encoding_config = ParameterConfig(parameter_name="whitespace_param", data_encoding="   ")

        with pytest.raises(ValueError, match=UNKNOWN_PARAMETER):
            ParameterFactory.create(whitespace_encoding_config)

    # --- Field Preservation ---

    def test_create_preserves_exact_parameter_name(self) -> None:
        special_name_config = ParameterConfig(
            parameter_name="My_Special_Parameter_123",
            data_encoding="wav"
        )

        created_parameter = ParameterFactory.create(special_name_config)

        assert created_parameter.parameter_name == "My_Special_Parameter_123"

    def test_create_preserves_exact_data_encoding(self) -> None:
        uppercase_encoding_config = ParameterConfig(parameter_name="audio_sample", data_encoding="WAV")

        created_parameter = ParameterFactory.create(uppercase_encoding_config)

        assert created_parameter.data_encoding == "WAV"