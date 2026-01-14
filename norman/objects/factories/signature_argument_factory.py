from norman_utils_external.encoding_arguments import EncodingArguments
from norman_utils_external.singleton import Singleton


class SignatureArgumentFactory(metaclass=Singleton):

    @staticmethod
    def create(argument_config: dict[str, str], container_encoding: str) -> dict[str, str]:
        signature_arguments = {}
        if container_encoding in EncodingArguments.Arguments_Map:
            signature_arguments |= EncodingArguments.Arguments_Map[container_encoding]

        signature_arguments |= argument_config
        normalized_arguments = {key: str(value) for key, value in signature_arguments.items()}

        return normalized_arguments
