from norman_objects.shared.encoding.channel_encoding import ChannelEncoding
from norman_objects.shared.encoding.container_encoding import ContainerEncoding
from norman_objects.shared.encoding.sample_encoding import SampleEncoding
from norman_objects.shared.encoding.tensor_encoding import TensorEncoding
from norman_objects.shared.modality.channel_modality import ChannelModality
from norman_objects.shared.modality.container_modality import ContainerModality
from norman_objects.shared.parameters.model_parameter import ModelParameter
from norman_utils_external.encoding_combinations import EncodingCombinations
from norman_utils_external.encoding_defaults import EncodingDefaults
from norman_utils_external.singleton import Singleton

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.parameter_argument_factory import ParameterArgumentFactory


class ParameterFactory(metaclass=Singleton):
    @staticmethod
    def create(parameter_config: ParameterConfig, container_modality: ContainerModality, container_encoding: ContainerEncoding) -> ModelParameter:
        if container_modality is None:
            raise ValueError("Signature container modality must be resolved by the signature factory")

        if container_encoding is None:
            raise ValueError("Signature container encoding must be resolved by the signature factory")

        if container_modality not in EncodingDefaults.Channel_Map:
            raise KeyError("Signature container modality has no default container encodings")

        default_container_encodings = EncodingDefaults.Channel_Map[container_modality]
        if container_encoding not in default_container_encodings:
            raise KeyError("Signature container encoding has no default channel modalities")

        default_channel_modalities = default_container_encodings[container_encoding]
        channel_modality_name = parameter_config.channel_modality
        if channel_modality_name is None:
            raise ValueError("Parameter channel modality cannot be None")

        channel_modality = ChannelModality(channel_modality_name)
        default_channel_encodings = default_channel_modalities[channel_modality]

        channel_encoding_name = parameter_config.channel_encoding
        if channel_encoding_name is None:
            channel_encoding_name = default_channel_encodings["channel"]

        sample_encoding_name = parameter_config.sample_encoding
        if sample_encoding_name is None:
            sample_encoding_name = default_channel_encodings["sample"]

        tensor_encoding_name = parameter_config.tensor_encoding
        if tensor_encoding_name is None:
            tensor_encoding_name = default_channel_encodings["tensor"]

        if container_modality not in EncodingCombinations.Combinations_Map:
            raise KeyError("Signature container modality is not supported")

        supported_container_encodings = EncodingCombinations.Combinations_Map[container_modality]
        if container_encoding not in supported_container_encodings:
            raise KeyError("Signature container encoding is not supported for the resolved container modality")

        supported_channel_modalities = supported_container_encodings[container_encoding]
        if channel_modality not in supported_channel_modalities:
            raise KeyError("Parameter channel modality is not supported for the resolved container encoding")

        channel_encoding = ChannelEncoding(channel_encoding_name)
        supported_channel_encodings = supported_channel_modalities[channel_modality]

        if channel_encoding not in supported_channel_encodings:
            raise KeyError("Parameter channel encoding is not supported for the resolved channel modality")

        sample_encoding = SampleEncoding(sample_encoding_name)
        supported_sample_encodings = supported_channel_encodings[channel_encoding]

        if sample_encoding not in supported_sample_encodings:
            raise KeyError("Parameter sample encoding is not supported for the resolved channel encoding")


        tensor_encoding = TensorEncoding(tensor_encoding_name)

        parameter_arguments = ParameterArgumentFactory.create(
            parameter_config.arguments,
            container_modality,
            container_encoding,
            channel_modality,
            channel_encoding,
            sample_encoding,
        )

        model_parameter = ModelParameter(
            id=parameter_config.id,
            model_id=parameter_config.model_id,
            version_id=parameter_config.version_id,
            signature_id=parameter_config.signature_id,
            channel_modality=channel_modality,
            channel_encoding=channel_encoding,
            sample_encoding=sample_encoding,
            tensor_encoding=tensor_encoding,
            name=parameter_config.name,
            arguments=parameter_arguments
        )

        return model_parameter
