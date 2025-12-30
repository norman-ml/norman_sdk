from typing import Any

from norman_utils_external.singleton import Singleton

from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.resolvers.input_source_resolver import InputSourceResolver


class InvocationConfigFactory(metaclass=Singleton):

    @staticmethod
    def create(invocation_config: dict[str, Any]) -> InvocationConfig:
        invocation_config = InvocationConfig.model_validate(invocation_config)

        for invocation_input in invocation_config.inputs:
            if invocation_input.source is None:
                invocation_input.source = InputSourceResolver.resolve(invocation_input.data)

        return invocation_config
