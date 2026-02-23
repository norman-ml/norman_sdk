from norman_utils_external.singleton import Singleton


class SignatureArgumentFactory(metaclass=Singleton):

    @staticmethod
    def create(argument_config: dict[str, str], container_encoding_name: str) -> dict[str, str]:
        # We currently do not have default signature arguments
        # but we keep the factory for forward compatibility
        normalized_arguments = {key: str(value) for key, value in argument_config.items()}
        return normalized_arguments
