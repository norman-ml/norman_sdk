from norman_objects.shared.parameters.data_modality import DataModality
from norman_utils_external.parameter_modality_mapping import ParameterModalityMapping


class ParameterModalityResolver:

    @staticmethod
    def resolve(encoding: str) -> DataModality:
        if encoding is None or not isinstance(encoding, str):
            raise ValueError("encoding must be a non-empty string")

        stripped_encoding = encoding.lower().strip()
        if stripped_encoding not in ParameterModalityMapping.Encoding_Map:
            raise ValueError(f"Unknown parameter encoding: {stripped_encoding}")

        return ParameterModalityMapping.Encoding_Map.get[stripped_encoding]
