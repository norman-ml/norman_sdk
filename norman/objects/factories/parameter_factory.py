from norman_objects.shared.parameters.model_parameter import ModelParameter
from norman_utils_external.singleton import Singleton

from norman.objects.configs.model.parameter_config import ParameterConfig


class ParameterFactory(metaclass=Singleton):

    @staticmethod
    def create(parameter_config: ParameterConfig) -> ModelParameter:
        channel_encoding = parameter_config.container_encoding
        if channel_encoding is None:
            pass # TODO implement

        sample_encoding = parameter_config.sample_encoding
        if sample_encoding is None:
            pass  # TODO implement

        tensor_encoding = parameter_config.tensor_encoding
        if tensor_encoding is None:
            pass  # TODO implement

        model_param = ModelParameter(
            id=parameter_config.id,
            model_id=parameter_config.model_id,
            version_id=parameter_config.version_id,
            signature_id=parameter_config.signature_id,
            parameter_name=parameter_config.parameter_name,
            channel_modality=parameter_config.channel_modality,
            channel_encoding=channel_encoding,
            sample_encoding=sample_encoding,
            tensor_encoding=tensor_encoding
        )

        return model_param
