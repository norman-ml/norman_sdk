from norman_objects.shared.parameters.model_param import ModelParam
from norman_utils_external.singleton import Singleton

from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.resolvers.parameter_modality_resolver import ParameterModalityResolver


class ParameterFactory(metaclass=Singleton):

    @staticmethod
    def create(parameter_config: ParameterConfig) -> ModelParam:
        data_modality = ParameterModalityResolver.resolve(parameter_config.data_encoding)
        model_param = ModelParam(
            data_encoding=parameter_config.data_encoding,
            parameter_name=parameter_config.parameter_name,
            data_modality=data_modality
        )

        return model_param
