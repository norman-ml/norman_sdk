from norman_objects.shared.models.model_projection import ModelProjection
from norman_utils_external.singleton import Singleton

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.factories.model_version_factory import ModelVersionFactory
from norman.objects.factories.tag_factory import TagFactory


class ModelProjectionFactory(metaclass=Singleton):
    authentication_manager = AuthenticationManager()

    @staticmethod
    def create(model_config: ModelProjectionConfig) -> ModelProjection:
        category = model_config.category
        if category is None:
            category = ""

        user_tags = []
        if model_config.user_tags is not None:
            for tag_config in model_config.user_tags:
                created = TagFactory.create(tag_config)
                user_tags.append(created)

        model_version = ModelVersionFactory.create(model_config.version)

        model = ModelProjection(
            account_id=ModelProjectionFactory.authentication_manager.account_id,
            name=model_config.name,
            category=category,
            invocation_count=0,
            version=model_version,
            user_tags=user_tags
        )

        if model_config.id is not None:
            model.id = model_config.id

        return model
