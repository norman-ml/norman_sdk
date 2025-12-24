import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.resolvers.signature_modality_resolver import SignatureModalityResolver

UNKNOWN_SIGNATURE_ENCODING = "Unknown signature encoding"

class TestSignatureModalityResolver:
    @pytest.mark.parametrize("encoding", ["aac", "ac3", "flac", "mp3", "opus", "vorbis", "wav"])
    def test_resolve_audio_encoding_returns_audio(self, encoding: str) -> None:
        resolved_modality = SignatureModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Audio

    @pytest.mark.parametrize("encoding", ["jpg", "jpeg", "png", "webp"])
    def test_resolve_image_encoding_returns_image(self, encoding: str) -> None:
        resolved_modality = SignatureModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Image

    @pytest.mark.parametrize("encoding", ["txt", "utf8", "utf16"])
    def test_resolve_text_encoding_returns_text(self, encoding: str) -> None:
        resolved_modality = SignatureModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Text

    @pytest.mark.parametrize("encoding", ["avi", "matroska", "mov", "mp4", "ogg", "webm"])
    def test_resolve_video_encoding_returns_video(self, encoding: str):
        resolved_modality = SignatureModalityResolver.resolve(encoding)

        assert resolved_modality == DataModality.Video

    @pytest.mark.parametrize("encoding", ["WAV", "Wav", "  wav  ", "  MP4  "])
    def test_resolve_handles_case_and_whitespace(self, encoding: str) -> None:
        resolved_modality = SignatureModalityResolver.resolve(encoding)

        assert resolved_modality in [DataModality.Audio, DataModality.Video]

    def test_resolve_none_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(None)

    @pytest.mark.parametrize("invalid_input", [123, ["mp4"], {"encoding": "mp4"}, b"mp4", True])
    def test_resolve_non_string_raises_value_error(self, invalid_input) -> None:
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(invalid_input)

    def test_resolve_unknown_encoding_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_SIGNATURE_ENCODING):
            SignatureModalityResolver.resolve("unknown_format")

    def test_resolve_empty_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_SIGNATURE_ENCODING):
            SignatureModalityResolver.resolve("")

    def test_resolve_whitespace_only_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_SIGNATURE_ENCODING):
            SignatureModalityResolver.resolve("   ")

    @pytest.mark.parametrize("subtitle_format", ["srt", "ass", "vtt", "mp4_text"])
    def test_resolve_subtitle_formats_not_supported(self, subtitle_format: str) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_SIGNATURE_ENCODING):
            SignatureModalityResolver.resolve(subtitle_format)

    @pytest.mark.parametrize("codec_format", ["h264", "h265", "av1", "vp8", "vp9"])
    def test_resolve_codec_formats_not_supported(self, codec_format: str) -> None:
        with pytest.raises(ValueError, match=UNKNOWN_SIGNATURE_ENCODING):
            SignatureModalityResolver.resolve(codec_format)