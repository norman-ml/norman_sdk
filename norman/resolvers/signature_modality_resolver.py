from norman_objects.shared.parameters.data_modality import DataModality
from norman_utils_external.signature_modality_mapping import SignatureModalityMapping

class SignatureModalityResolver:

    @staticmethod
    def resolve(encoding: str) -> DataModality:
        if encoding is None or not isinstance(encoding, str):
            raise ValueError("encoding must be a non-empty string")

        stripped_encoding = encoding.lower().strip()
        if stripped_encoding not in SignatureModalityMapping._Encoding_Map:
            raise ValueError(f"Unknown signature encoding: {stripped_encoding}")

        return SignatureModalityMapping._Encoding_Map[stripped_encoding]
