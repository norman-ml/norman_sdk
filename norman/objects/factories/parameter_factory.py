from norman_objects.shared.encoding.channel_encoding import ChannelEncoding
from norman_objects.shared.encoding.sample_encoding import SampleEncoding
from norman_objects.shared.encoding.tensor_encoding import TensorEncoding
from norman_objects.shared.modality.channel_modality import ChannelModality
from norman_objects.shared.parameters.model_parameter import ModelParameter
from norman_utils_external.encoding_combinations import EncodingCombinations
from norman_utils_external.encoding_defaults import EncodingDefaults
from norman_utils_external.singleton import Singleton

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.parameter_argument_factory import ParameterArgumentFactory


class ParameterFactory(metaclass=Singleton):
    @staticmethod
    def create(parameter_config: ParameterConfig, container_modality_name: str, container_encoding_name: str) -> ModelParameter:
        if container_modality_name is None:
            raise ValueError("Signature container modality must be resolved by the signature factory")

        if container_encoding_name is None:
            raise ValueError("Signature container encoding must be resolved by the signature factory")

        if container_modality_name not in EncodingDefaults.Channel_Map:
            raise KeyError("Signature container modality has no default container encodings")

        default_container_encodings = EncodingDefaults.Channel_Map[container_modality_name]
        if container_encoding_name not in default_container_encodings:
            raise KeyError("Signature container encoding has no default channel modalities")

        default_channel_modalities = default_container_encodings[container_encoding_name]
        channel_modality_name = parameter_config.channel_modality
        if channel_modality_name not in default_channel_modalities:
            raise KeyError("Parameter channel modality has no default channel encodings")

        default_channel_encodings = default_channel_modalities[channel_modality_name]
        channel_encoding_name = parameter_config.channel_encoding
        if channel_encoding_name is None:
            channel_encoding_name = default_channel_encodings["channel"]

        sample_encoding_name = parameter_config.sample_encoding
        if sample_encoding_name is None:
            sample_encoding_name = default_channel_encodings["sample"]

        tensor_encoding_name = parameter_config.tensor_encoding
        if tensor_encoding_name is None:
            tensor_encoding_name = default_channel_encodings["tensor"]

        if container_modality_name not in EncodingCombinations.Combinations_Map:
            raise KeyError("Signature container modality is not supported")

        supported_container_encodings = EncodingCombinations.Combinations_Map[container_modality_name]
        if container_encoding_name not in supported_container_encodings:
            raise KeyError("Signature container encoding is not supported for the resolved container modality")

        supported_channel_modalities = supported_container_encodings[container_encoding_name]
        if channel_modality_name not in supported_channel_modalities:
            raise KeyError("Parameter channel modality is not supported for the resolved container encoding")

        supported_channel_encodings = supported_channel_modalities[channel_encoding_name]
        if sample_encoding_name not in supported_channel_encodings:
            raise KeyError("Parameter channel encoding is not supported for the resolved channel modality")

        parameter_arguments = ParameterArgumentFactory.create(
            parameter_config.arguments,
            container_modality_name,
            container_encoding_name,
            channel_modality_name,
            channel_encoding_name,
            sample_encoding_name,
        )

        model_parameter = ModelParameter(
            id=parameter_config.id,
            model_id=parameter_config.model_id,
            version_id=parameter_config.version_id,
            signature_id=parameter_config.signature_id,
            channel_modality=ChannelModality(channel_modality_name),
            channel_encoding=ChannelEncoding(channel_encoding_name),
            sample_encoding=SampleEncoding(sample_encoding_name),
            tensor_encoding=TensorEncoding(tensor_encoding_name),
            name=parameter_config.name,
            arguments=parameter_arguments
        )

        return model_parameter
