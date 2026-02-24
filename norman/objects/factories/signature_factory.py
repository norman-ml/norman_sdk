from norman_objects.shared.encoding.container_encoding import ContainerEncoding
from norman_objects.shared.modality.container_modality import ContainerModality
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
        container_modality_name = signature_config.container_modality
        if container_modality_name is None:
            raise ValueError("Signature container modality cannot be None")

        if container_modality_name not in EncodingCombinations.Combinations_Map:
            raise KeyError("Signature container modality is not supported")

        container_encoding_name = signature_config.container_encoding
        if container_encoding_name is None:
            if container_modality_name not in EncodingDefaults.Container_Map[container_modality_name]:
                raise KeyError("Signature container modality has no default container encodings")

            container_encoding_name = EncodingDefaults.Container_Map[container_modality_name]

        supported_container_encodings = EncodingCombinations.Combinations_Map[container_modality_name]
        if container_encoding_name not in supported_container_encodings:
            raise KeyError("Signature container encoding is not supported for the resolved container modality")

        http_location = signature_config.http_location
        if signature_config.http_location is None:
            http_location = HttpLocation.Body

        hidden = signature_config.hidden
        if signature_config.hidden is None:
            hidden = False

        parameters = []
        for parameter_config in signature_config.parameters:
            parameter = ParameterFactory.create(parameter_config, container_modality_name, container_encoding_name)
            parameters.append(parameter)

        # Currently not defined by users, defined for completeness
        transforms = []
        
        arguments = SignatureArgumentFactory.create(signature_config.arguments, signature_config.container_encoding)

        model_signature = ModelSignature(
            id=signature_config.id,
            model_id=signature_config.model_id,
            version_id=signature_config.version_id,
            signature_type=signature_type,
            container_modality=ContainerModality(container_modality_name),
            data_domain=signature_config.data_domain,
            container_encoding=ContainerEncoding(container_encoding_name),
            receive_format=signature_config.receive_format,
            http_location=HttpLocation[http_location],
            hidden=hidden,
            display_title=signature_config.display_title,
            default_value=signature_config.default_value,
            parameters=parameters,
            transforms=transforms,
            arguments=arguments
        )

        return model_signature
