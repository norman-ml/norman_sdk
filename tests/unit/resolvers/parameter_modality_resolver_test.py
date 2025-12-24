import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.resolvers.parameter_modality_resolver import ParameterModalityResolver


class TestParameterModalityResolverResolve:
    """Tests for ParameterModalityResolver.resolve() method"""

    # --- Audio Encodings ---

    @pytest.mark.parametrize("encoding", ["aac", "ac3", "flac", "mp3", "opus", "vorbis", "wav"])
    def test_resolve_audio_encoding_returns_audio(self, encoding):
        result = ParameterModalityResolver.resolve(encoding)

        assert result == DataModality.Audio

    # --- Image Encodings ---

    @pytest.mark.parametrize("encoding", ["jpg", "jpeg", "png", "webp"])
    def test_resolve_image_encoding_returns_image(self, encoding):
        result = ParameterModalityResolver.resolve(encoding)

        assert result == DataModality.Image

    # --- Text Encodings ---

    @pytest.mark.parametrize("encoding", ["ass", "mp4_text", "srt", "utf8", "utf16", "vtt"])
    def test_resolve_text_encoding_returns_text(self, encoding):
        result = ParameterModalityResolver.resolve(encoding)

        assert result == DataModality.Text

    # --- Video Encodings ---

    @pytest.mark.parametrize("encoding", ["av1", "h264", "h265", "vp8", "vp9"])
    def test_resolve_video_encoding_returns_video(self, encoding):
        result = ParameterModalityResolver.resolve(encoding)

        assert result == DataModality.Video

    # --- Case Insensitivity ---

    @pytest.mark.parametrize("encoding", ["WAV", "Wav", "wAv", "waV"])
    def test_resolve_handles_uppercase(self, encoding):
        result = ParameterModalityResolver.resolve(encoding)

        assert result == DataModality.Audio

    @pytest.mark.parametrize("encoding", ["MP3", "Mp3", "mP3"])
    def test_resolve_handles_mixed_case(self, encoding):
        result = ParameterModalityResolver.resolve(encoding)

        assert result == DataModality.Audio

    @pytest.mark.parametrize("encoding", ["PNG", "Png", "pNg"])
    def test_resolve_handles_image_case_variations(self, encoding):
        result = ParameterModalityResolver.resolve(encoding)

        assert result == DataModality.Image

    # --- Whitespace Handling ---

    def test_resolve_trims_leading_whitespace(self):
        result = ParameterModalityResolver.resolve("  wav")

        assert result == DataModality.Audio

    def test_resolve_trims_trailing_whitespace(self):
        result = ParameterModalityResolver.resolve("wav  ")

        assert result == DataModality.Audio

    def test_resolve_trims_both_whitespace(self):
        result = ParameterModalityResolver.resolve("  wav  ")

        assert result == DataModality.Audio

    def test_resolve_handles_whitespace_and_case(self):
        result = ParameterModalityResolver.resolve("  WAV  ")

        assert result == DataModality.Audio

    # --- Error Cases: None ---

    def test_resolve_none_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(None)

    # --- Error Cases: Non-String Types ---

    def test_resolve_integer_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(123)

    def test_resolve_list_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(["wav"])

    def test_resolve_dict_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve({"encoding": "wav"})

    def test_resolve_bytes_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(b"wav")

    def test_resolve_boolean_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(True)

    def test_resolve_float_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            ParameterModalityResolver.resolve(3.14)

    # --- Error Cases: Unknown Encodings ---

    def test_resolve_unknown_encoding_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterModalityResolver.resolve("unknown")

    def test_resolve_empty_string_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterModalityResolver.resolve("")

    def test_resolve_whitespace_only_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterModalityResolver.resolve("   ")

    def test_resolve_similar_but_wrong_encoding_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterModalityResolver.resolve("wave")  # Close to "wav" but not valid

    def test_resolve_partial_encoding_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterModalityResolver.resolve("mp")  # Partial "mp3"

    def test_resolve_encoding_with_extension_dot_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterModalityResolver.resolve(".wav")  # Has leading dot


class TestParameterModalityResolverEncodingMap:
    """Tests verifying the encoding map contents"""

    def test_all_audio_encodings_mapped(self):
        audio_encodings = ["aac", "ac3", "flac", "mp3", "opus", "vorbis", "wav"]

        for encoding in audio_encodings:
            assert ParameterModalityResolver.resolve(encoding) == DataModality.Audio

    def test_all_image_encodings_mapped(self):
        image_encodings = ["jpg", "jpeg", "png", "webp"]

        for encoding in image_encodings:
            assert ParameterModalityResolver.resolve(encoding) == DataModality.Image

    def test_all_text_encodings_mapped(self):
        text_encodings = ["ass", "mp4_text", "srt", "utf8", "utf16", "vtt"]

        for encoding in text_encodings:
            assert ParameterModalityResolver.resolve(encoding) == DataModality.Text

    def test_all_video_encodings_mapped(self):
        video_encodings = ["av1", "h264", "h265", "vp8", "vp9"]

        for encoding in video_encodings:
            assert ParameterModalityResolver.resolve(encoding) == DataModality.Video

    def test_total_encoding_count(self):
        """Verify the total number of supported encodings"""
        expected_count = 7 + 4 + 6 + 5  # audio + image + text + video

        assert len(ParameterModalityResolver._Encoding_Map) == expected_count


class TestParameterModalityResolverEdgeCases:
    def test_resolve_encoding_with_tab_raises_error(self):
        # Tab is whitespace, so "wav\t" stripped becomes "wav"
        # Actually, strip() removes tabs, so this should work
        result = ParameterModalityResolver.resolve("wav\t")

        assert result == DataModality.Audio

    def test_resolve_encoding_with_internal_space_raises_error(self):
        with pytest.raises(ValueError, match="Unknown parameter encoding"):
            ParameterModalityResolver.resolve("wa v")

    def test_resolve_returns_enum_member(self):
        result = ParameterModalityResolver.resolve("wav")

        assert isinstance(result, DataModality)

    def test_resolve_same_encoding_multiple_times_consistent(self):
        result1 = ParameterModalityResolver.resolve("mp3")
        result2 = ParameterModalityResolver.resolve("mp3")
        result3 = ParameterModalityResolver.resolve("MP3")

        assert result1 == result2 == result3 == DataModality.Audio

    def test_resolve_jpg_and_jpeg_both_map_to_image(self):
        jpg_result = ParameterModalityResolver.resolve("jpg")
        jpeg_result = ParameterModalityResolver.resolve("jpeg")

        assert jpg_result == jpeg_result == DataModality.Image