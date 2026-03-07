from typing import Optional

from norman_utils_external.singleton import Singleton


class SignatureArgumentFactory(metaclass=Singleton):

    @staticmethod
    def create(argument_config: Optional[dict[str, str]], container_encoding_name: str) -> dict[str, str]:
        if argument_config is None:
            argument_config = {}

        # We currently do not have default signature arguments,
        # but we keep the factory for forward compatibility
        normalized_arguments = {key: str(value) for key, value in argument_config.items()}
        return normalized_arguments
