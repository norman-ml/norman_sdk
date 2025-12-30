from norman_objects.shared.parameters.data_modality import DataModality


class ParameterModalityResolver:
    __Encoding_Map = {
        # Audio (frame level)
        "aac": DataModality.Audio,
        "ac3": DataModality.Audio,
        "flac": DataModality.Audio,
        "mp3": DataModality.Audio,
        "opus": DataModality.Audio,
        "vorbis": DataModality.Audio,
        "wav": DataModality.Audio,

        # Image (frame level)
        "jpg": DataModality.Image,
        "jpeg": DataModality.Image,
        "png": DataModality.Image,
        "webp": DataModality.Image,

        # Text (frame level)
        "ass": DataModality.Text,
        "mp4_text": DataModality.Text,
        "srt": DataModality.Text,
        "utf8": DataModality.Text,
        "utf16": DataModality.Text,
        "vtt": DataModality.Text,

        # Video (frame level)
        "av1": DataModality.Video,
        "h264": DataModality.Video,
        "h265": DataModality.Video,
        "vp8": DataModality.Video,
        "vp9": DataModality.Video
    }

    @staticmethod
    def resolve(encoding: str) -> DataModality:
        if encoding is None or not isinstance(encoding, str):
            raise ValueError("encoding must be a non-empty string")

        stripped_encoding = encoding.lower().strip()
        if stripped_encoding not in ParameterModalityResolver.__Encoding_Map:
            raise ValueError(f"Unknown parameter encoding: {stripped_encoding}")

        return ParameterModalityResolver.__Encoding_Map[stripped_encoding]
