from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_utils_external.singleton import Singleton

from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.factories.parameter_factory import ParameterFactory


class SignatureFactory(metaclass=Singleton):

    @staticmethod
    def create(signature_config: SignatureConfig, signature_type: SignatureType) -> ModelSignature:
        if signature_config.container_encoding is not None:
            container_encoding = signature_config.container_encoding
        else:
            container_encoding = None # TODO tmp until default resolver

        http_location = HttpLocation.Body
        if signature_config.http_location is not None:
            http_location = signature_config.http_location

        hidden = False
        if signature_config.hidden is not None:
            hidden = signature_config.hidden

        default_value = None
        if signature_config.default_value is not None:
            default_value = signature_config.default_value

        parameters = []
        for parameter in signature_config.parameters:
            parameters.append(ParameterFactory.create(parameter))

        # Currently not defined by users, defined for completeness
        transforms = []
        signature_args = {}

        model_signature = ModelSignature(
            id=signature_config.id,
            model_id=signature_config.model_id,
            version_id=signature_config.version_id,
            signature_type=signature_type,
            data_modality=signature_config.data_modality,
            data_domain=signature_config.data_domain,
            container_encoding=container_encoding,
            receive_format=signature_config.receive_format,
            http_location=http_location,
            hidden=hidden,
            display_title=signature_config.display_title,
            default_value=default_value,
            parameters=parameters,
            transforms=transforms,
            signature_args=signature_args
        )

        return model_signature
