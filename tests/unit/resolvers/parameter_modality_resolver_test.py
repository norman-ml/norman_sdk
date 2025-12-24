import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.resolvers.parameter_modality_resolver import ParameterModalityResolver

UNKNOWN_PARAMETER_ENCODING = "Unknown parameter encoding"

class TestParameterModalityResolver:
    @pytest.mark.parametrize("encoding", ["aac", "ac3", "flac", "mp3", "opus", "vorbis", "wav"])
    def test_resolve_audio_encoding_returns_audio(self, encoding: str) -> None:
        resolved_modality = ParameterModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Audio
        
    @pytest.mark.parametrize("encoding", ["jpg", "jpeg", "png", "webp"])
    def test_resolve_image_encoding_returns_image(self, encoding: str) -> None:
        resolved_modality = ParameterModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Image

    @pytest.mark.parametrize("encoding", ["ass", "mp4_text", "srt", "utf8", "utf16", "vtt"])
    def test_resolve_text_encoding_returns_text(self, encoding: str) -> None:
        resolved_modality = ParameterModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Text

    @pytest.mark.parametrize("encoding", ["av1", "h264", "h265", "vp8", "vp9"])
    def test_resolve_video_encoding_returns_video(self, encoding: str) -> None:
        resolved_modality = ParameterModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Video

    @pytest.mark.parametrize("encoding", ["WAV", "Wav", "  wav  ", "  WAV  "])
    def test_resolve_handles_case_and_whitespace(self, encoding: str) -> None:
        resolved_modality = ParameterModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Audio

    def test_resolve_none_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(None)

    @pytest.mark.parametrize("invalid_input", [123, ["wav"], {"encoding": "wav"}, b"wav", True])
    def test_resolve_non_string_raises_value_error(self, invalid_input) -> None:
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(invalid_input)

    def test_resolve_unknown_encoding_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_PARAMETER_ENCODING):
            ParameterModalityResolver.resolve("unknown_format")

    def test_resolve_empty_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_PARAMETER_ENCODING):
            ParameterModalityResolver.resolve("")

    def test_resolve_whitespace_only_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_PARAMETER_ENCODING):
            ParameterModalityResolver.resolve("   ")