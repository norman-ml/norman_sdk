from typing import Optional

from norman_objects.shared.encoding.channel_encoding import ChannelEncoding
from norman_objects.shared.encoding.container_encoding import ContainerEncoding
from norman_objects.shared.encoding.sample_encoding import SampleEncoding
from norman_objects.shared.modality.channel_modality import ChannelModality
from norman_objects.shared.modality.container_modality import ContainerModality
from norman_utils.encoding_arguments import EncodingArguments
from norman_utils.singleton import Singleton


class ParameterArgumentFactory(metaclass=Singleton):
    @staticmethod
    def create(
            argument_config: Optional[dict[str, str]],
            container_modality: ContainerModality,
            container_encoding: ContainerEncoding,
            channel_modality: ChannelModality,
            channel_encoding: ChannelEncoding,
            sample_encoding: SampleEncoding
        ) -> dict[str, str]:

        if argument_config is None:
            argument_config = {}

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
