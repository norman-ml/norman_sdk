from norman_utils_external.encoding_arguments import EncodingArguments
from norman_utils_external.singleton import Singleton


class ParameterArgumentFactory(metaclass=Singleton):

    @staticmethod
    def create(
            argument_config: dict[str, str],
            container_modality: str,
            container_encoding: str,
            channel_modality: str,
            channel_encoding: str,
            sample_encoding: str
        ) -> dict[str, str]:

        arguments_map = EncodingArguments.Arguments_Map
        if container_modality not in arguments_map:
            return argument_config

        container_modality_submap = arguments_map[container_modality]
        if container_encoding not in container_modality_submap:
            return argument_config

        container_encoding_submap = container_modality_submap[container_encoding]
        if channel_modality not in container_encoding_submap:
            return argument_config

        channel_modality_submap = container_encoding_submap[channel_modality]
        if channel_encoding not in channel_modality_submap:
            return argument_config

        channel_encoding_submap = channel_modality_submap[channel_encoding]

        arguments = argument_config | channel_encoding_submap
        normalized_arguments = {key: str(value) for key, value in arguments.items()}

        return normalized_arguments
