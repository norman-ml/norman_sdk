from norman_objects.shared.parameters.model_parameter import ModelParameter
from norman_utils_external.singleton import Singleton

from norman.objects.configs.model.parameter_config import ParameterConfig


class ParameterFactory(metaclass=Singleton):

    @staticmethod
    def create(parameter_config: ParameterConfig) -> ModelParameter:
        if parameter_config.channel_encoding is not None:
            channel_encoding = parameter_config.channel_encoding
        else:
            channel_encoding = None # TODO tmp until default resolver

        if parameter_config.sample_encoding is not None:
            sample_encoding = parameter_config.sample_encoding
        else:
            sample_encoding = None # TODO tmp until default resolver

        if parameter_config.tensor_encoding is not None:
            tensor_encoding = parameter_config.tensor_encoding
        else:
            tensor_encoding = None # TODO tmp until default resolver

        model_param = ModelParameter(
            id=parameter_config.id,
            model_id=parameter_config.model_id,
            version_id=parameter_config.version_id,
            signature_id=parameter_config.signature_id,
            channel_encoding=channel_encoding,
            sample_encoding=sample_encoding,
            tensor_encoding=tensor_encoding,
            parameter_name=parameter_config.parameter_name
        )

        return model_param
