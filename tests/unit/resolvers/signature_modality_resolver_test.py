import pytest

from norman_objects.shared.parameters.data_modality import DataModality

from norman.resolvers.signature_modality_resolver import SignatureModalityResolver


class TestSignatureModalityResolverResolve:
    """Tests for SignatureModalityResolver.resolve() method"""

    # --- Audio Encodings ---

    @pytest.mark.parametrize("encoding", ["aac", "ac3", "flac", "mp3", "opus", "vorbis", "wav"])
    def test_resolve_audio_encoding_returns_audio(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Audio

    # --- Image Encodings ---

    @pytest.mark.parametrize("encoding", ["jpg", "jpeg", "png", "webp"])
    def test_resolve_image_encoding_returns_image(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Image

    # --- Text Encodings ---

    @pytest.mark.parametrize("encoding", ["txt", "utf8", "utf16"])
    def test_resolve_text_encoding_returns_text(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Text

    # --- Video Encodings ---

    @pytest.mark.parametrize("encoding", ["avi", "matroska", "mov", "mp4", "ogg", "webm"])
    def test_resolve_video_encoding_returns_video(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Video

    # --- Case Insensitivity ---

    @pytest.mark.parametrize("encoding", ["WAV", "Wav", "wAv", "waV"])
    def test_resolve_handles_uppercase(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Audio

    @pytest.mark.parametrize("encoding", ["MP4", "Mp4", "mP4"])
    def test_resolve_handles_mixed_case_video(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Video

    @pytest.mark.parametrize("encoding", ["PNG", "Png", "pNg"])
    def test_resolve_handles_image_case_variations(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Image

    @pytest.mark.parametrize("encoding", ["TXT", "Txt", "tXt"])
    def test_resolve_handles_text_case_variations(self, encoding):
        result = SignatureModalityResolver.resolve(encoding)

        assert result == DataModality.Text

    # --- Whitespace Handling ---

    def test_resolve_trims_leading_whitespace(self):
        result = SignatureModalityResolver.resolve("  wav")

        assert result == DataModality.Audio

    def test_resolve_trims_trailing_whitespace(self):
        result = SignatureModalityResolver.resolve("wav  ")

        assert result == DataModality.Audio

    def test_resolve_trims_both_whitespace(self):
        result = SignatureModalityResolver.resolve("  wav  ")

        assert result == DataModality.Audio

    def test_resolve_handles_whitespace_and_case(self):
        result = SignatureModalityResolver.resolve("  MP4  ")

        assert result == DataModality.Video

    # --- Error Cases: None ---

    def test_resolve_none_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(None)

    # --- Error Cases: Non-String Types ---

    def test_resolve_integer_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(123)

    def test_resolve_list_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(["mp4"])

    def test_resolve_dict_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve({"encoding": "mp4"})

    def test_resolve_bytes_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(b"mp4")

    def test_resolve_boolean_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(True)

    def test_resolve_float_raises_value_error(self):
        with pytest.raises(ValueError, match="encoding must be a non-empty string"):
            SignatureModalityResolver.resolve(3.14)

    # --- Error Cases: Unknown Encodings ---

    def test_resolve_unknown_encoding_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("unknown")

    def test_resolve_empty_string_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("")

    def test_resolve_whitespace_only_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("   ")

    def test_resolve_similar_but_wrong_encoding_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("wave")  # Close to "wav" but not valid

    def test_resolve_partial_encoding_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("mp")  # Partial

    def test_resolve_encoding_with_extension_dot_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve(".mp4")  # Has leading dot

    def test_resolve_parameter_encoding_not_in_signature_raises_error(self):
        """Encodings in ParameterModalityResolver but not in SignatureModalityResolver"""
        # These are in ParameterModalityResolver but not SignatureModalityResolver
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("srt")

        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("ass")

        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("h264")


class TestSignatureModalityResolverEncodingMap:
    """Tests verifying the encoding map contents"""

    def test_all_audio_encodings_mapped(self):
        audio_encodings = ["aac", "ac3", "flac", "mp3", "opus", "vorbis", "wav"]

        for encoding in audio_encodings:
            assert SignatureModalityResolver.resolve(encoding) == DataModality.Audio

    def test_all_image_encodings_mapped(self):
        image_encodings = ["jpg", "jpeg", "png", "webp"]

        for encoding in image_encodings:
            assert SignatureModalityResolver.resolve(encoding) == DataModality.Image

    def test_all_text_encodings_mapped(self):
        text_encodings = ["txt", "utf8", "utf16"]

        for encoding in text_encodings:
            assert SignatureModalityResolver.resolve(encoding) == DataModality.Text

    def test_all_video_encodings_mapped(self):
        video_encodings = ["avi", "matroska", "mov", "mp4", "ogg", "webm"]

        for encoding in video_encodings:
            assert SignatureModalityResolver.resolve(encoding) == DataModality.Video

    def test_total_encoding_count(self):
        """Verify the total number of supported encodings"""
        expected_count = 7 + 4 + 3 + 6  # audio + image + text + video

        assert len(SignatureModalityResolver._Encoding_Map) == expected_count

    def test_audio_encoding_count(self):
        audio_count = sum(
            1 for v in SignatureModalityResolver._Encoding_Map.values()
            if v == DataModality.Audio
        )
        assert audio_count == 7

    def test_image_encoding_count(self):
        image_count = sum(
            1 for v in SignatureModalityResolver._Encoding_Map.values()
            if v == DataModality.Image
        )
        assert image_count == 4

    def test_text_encoding_count(self):
        text_count = sum(
            1 for v in SignatureModalityResolver._Encoding_Map.values()
            if v == DataModality.Text
        )
        assert text_count == 3

    def test_video_encoding_count(self):
        video_count = sum(
            1 for v in SignatureModalityResolver._Encoding_Map.values()
            if v == DataModality.Video
        )
        assert video_count == 6


class TestSignatureModalityResolverEdgeCases:
    """Edge case tests for SignatureModalityResolver"""

    def test_resolve_encoding_with_tab_whitespace(self):
        # Tab is whitespace, strip() removes it
        result = SignatureModalityResolver.resolve("mp4\t")

        assert result == DataModality.Video

    def test_resolve_encoding_with_internal_space_raises_error(self):
        with pytest.raises(ValueError, match="Unknown signature encoding"):
            SignatureModalityResolver.resolve("mp 4")

    def test_resolve_returns_enum_member(self):
        result = SignatureModalityResolver.resolve("mp4")

        assert isinstance(result, DataModality)

    def test_resolve_same_encoding_multiple_times_consistent(self):
        result1 = SignatureModalityResolver.resolve("mp4")
        result2 = SignatureModalityResolver.resolve("mp4")
        result3 = SignatureModalityResolver.resolve("MP4")

        assert result1 == result2 == result3 == DataModality.Video

    def test_resolve_jpg_and_jpeg_both_map_to_image(self):
        jpg_result = SignatureModalityResolver.resolve("jpg")
        jpeg_result = SignatureModalityResolver.resolve("jpeg")

        assert jpg_result == jpeg_result == DataModality.Image

    def test_resolve_matroska_returns_video(self):
        """Matroska is a less common but valid video container"""
        result = SignatureModalityResolver.resolve("matroska")

        assert result == DataModality.Video

    def test_resolve_ogg_returns_video(self):
        """Ogg is mapped as video container in this resolver"""
        result = SignatureModalityResolver.resolve("ogg")

        assert result == DataModality.Video


class TestSignatureVsParameterModalityResolver:
    """Tests highlighting differences between Signature and Parameter resolvers"""

    def test_signature_has_container_video_formats(self):
        """SignatureModalityResolver uses container formats for video"""
        container_formats = ["avi", "matroska", "mov", "mp4", "ogg", "webm"]

        for fmt in container_formats:
            result = SignatureModalityResolver.resolve(fmt)
            assert result == DataModality.Video

    def test_signature_has_txt_encoding(self):
        """SignatureModalityResolver has 'txt' which ParameterModalityResolver doesn't"""
        result = SignatureModalityResolver.resolve("txt")

        assert result == DataModality.Text

    def test_signature_missing_subtitle_formats(self):
        """SignatureModalityResolver doesn't have subtitle formats like srt, ass, vtt"""
        subtitle_formats = ["srt", "ass", "vtt", "mp4_text"]

        for fmt in subtitle_formats:
            with pytest.raises(ValueError, match="Unknown signature encoding"):
                SignatureModalityResolver.resolve(fmt)

    def test_signature_missing_codec_formats(self):
        """SignatureModalityResolver doesn't have codec formats like h264, h265"""
        codec_formats = ["h264", "h265", "av1", "vp8", "vp9"]

        for fmt in codec_formats:
            with pytest.raises(ValueError, match="Unknown signature encoding"):
                SignatureModalityResolver.resolve(fmt)