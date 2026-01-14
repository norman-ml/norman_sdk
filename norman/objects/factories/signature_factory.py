from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_utils_external.encoding_combinations import EncodingCombinations
from norman_utils_external.encoding_defaults import EncodingDefaults
from norman_utils_external.singleton import Singleton

from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.factories.parameter_factory import ParameterFactory
from norman.objects.factories.signature_argument_factory import SignatureArgumentFactory


class SignatureFactory(metaclass=Singleton):
    @staticmethod
    def create(signature_config: SignatureConfig, signature_type: SignatureType) -> ModelSignature:
        container_modality_name = signature_config.container_modality.lower()
        if container_modality_name not in EncodingDefaults.Container_Map:
            raise KeyError("Signature container modality has no default container encodings")

        container_encoding = signature_config.container_encoding
        if container_encoding is None:
            container_encoding = EncodingDefaults.Container_Map[container_modality_name]

        if container_modality_name not in EncodingCombinations.Combinations_Map:
            raise KeyError("Signature container data modality is not supported")

        supported_container_encodings = EncodingCombinations.Combinations_Map[container_modality_name]
        if container_encoding not in supported_container_encodings:
            raise KeyError("Signature container encoding is not supported for the resolved container modality")

        http_location = signature_config.http_location
        if signature_config.http_location is None:
            http_location = HttpLocation.Body

        hidden = signature_config.hidden
        if signature_config.hidden is None:
            hidden = False

        parameters = []
        for parameter in signature_config.parameters:
            parameters.append(ParameterFactory.create(parameter, signature_config.container_modality, container_encoding))

        # Currently not defined by users, defined for completeness
        transforms = []
        signature_args = SignatureArgumentFactory.create(signature_config.signature_arguments, signature_config.container_encoding)

        model_signature = ModelSignature(
            id=signature_config.id,
            model_id=signature_config.model_id,
            version_id=signature_config.version_id,
            signature_type=signature_type,
            container_modality=signature_config.container_modality,
            data_domain=signature_config.data_domain,
            container_encoding=container_encoding,
            receive_format=signature_config.receive_format,
            http_location=http_location,
            hidden=hidden,
            display_title=signature_config.display_title,
            default_value=signature_config.default_value,
            parameters=parameters,
            transforms=transforms,
            signature_args=signature_args
        )

        return model_signature
