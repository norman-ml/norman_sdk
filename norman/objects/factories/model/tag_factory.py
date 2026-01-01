from norman_objects.shared.models.model_tag import ModelTag
from norman_utils_external.singleton import Singleton

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.model_tag_config import ModelTagConfig


class TagFactory(metaclass=Singleton):
    authentication_manager = AuthenticationManager()

    @staticmethod
    def create(tag_config: ModelTagConfig) -> ModelTag:
        model_tag = ModelTag(
            account_id=TagFactory.authentication_manager.account_id,
            name=tag_config.name
        )

        return model_tag
