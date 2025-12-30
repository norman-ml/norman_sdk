from norman_objects.shared.parameters.data_modality import DataModality


class SignatureModalityResolver:
    __Encoding_Map = {
        # Audio (container level)
        "aac": DataModality.Audio,
        "ac3": DataModality.Audio,
        "flac": DataModality.Audio,
        "mp3": DataModality.Audio,
        "opus": DataModality.Audio,
        "vorbis": DataModality.Audio,
        "wav": DataModality.Audio,

        # Image (container level)
        "jpg": DataModality.Image,
        "jpeg": DataModality.Image,
        "png": DataModality.Image,
        "webp": DataModality.Image,

        # Text
        "txt": DataModality.Text,
        "utf8": DataModality.Text,
        "utf16": DataModality.Text,

        # Video (container level)
        "avi": DataModality.Video,
        "matroska": DataModality.Video,
        "mov": DataModality.Video,
        "mp4": DataModality.Video,
        "ogg": DataModality.Video,
        "webm": DataModality.Video
    }

    @staticmethod
    def resolve(encoding: str) -> DataModality:
        if encoding is None or not isinstance(encoding, str):
            raise ValueError("encoding must be a non-empty string")

        stripped_encoding = encoding.lower().strip()
        if stripped_encoding not in SignatureModalityResolver.__Encoding_Map:
            raise ValueError(f"Unknown signature encoding: {stripped_encoding}")

        return SignatureModalityResolver.__Encoding_Map[stripped_encoding]
