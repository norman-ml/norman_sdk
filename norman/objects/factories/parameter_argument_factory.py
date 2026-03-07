from typing import Optional

from norman_utils_external.encoding_arguments import EncodingArguments
from norman_utils_external.singleton import Singleton


class ParameterArgumentFactory(metaclass=Singleton):
    @staticmethod
    def create(
            argument_config: Optional[dict[str, str]],
            container_modality_name: str,
            container_encoding_name: str,
            channel_modality_name: str,
            channel_encoding_name: str,
            sample_encoding_name: str
        ) -> dict[str, str]:

        if argument_config is None:
            argument_config = {}

        arguments_map = EncodingArguments.Arguments_Map
        if container_modality_name not in arguments_map:
            return argument_config

        container_modality_submap = arguments_map[container_modality_name]
        if container_encoding_name not in container_modality_submap:
            return argument_config

        container_encoding_submap = container_modality_submap[container_encoding_name]
        if channel_modality_name not in container_encoding_submap:
            return argument_config

        channel_modality_submap = container_encoding_submap[channel_modality_name]
        if channel_encoding_name not in channel_modality_submap:
            return argument_config

        channel_encoding_submap = channel_modality_submap[channel_encoding_name]

        arguments = argument_config | channel_encoding_submap
        normalized_arguments = {key: str(value) for key, value in arguments.items()}

        return normalized_arguments
