import pytest

from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig


@pytest.mark.unit
@pytest.mark.config
class TestModelProjectionConfig:
    def test_create_with_all_fields(self) -> None:
        name = "sentiment-analyzer"
        category = "natural-language-processing"
        version_label = "v1.0"
        tag_name = "production"

        model_version_config = ModelVersionConfig(
            label=version_label,
            short_description="Text classifier",
            long_description="A model for classifying text",
            assets=[],
            inputs=[],
            outputs=[],
        )
        model_tag_config = ModelTagConfig(name=tag_name)
        model_projection_config = ModelProjectionConfig(
            name=name,
            category=category,
            version=model_version_config,
            user_tags=[model_tag_config],
        )

        assert model_projection_config.name == name
        assert model_projection_config.category == category
        assert model_projection_config.version.label == version_label
        assert len(model_projection_config.user_tags) == 1
        assert model_projection_config.user_tags[0].name == tag_name

    def test_create_without_optional_fields(self) -> None:
        name = "image-classifier"
        category = "computer-vision"

        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Image classifier",
            long_description="A CNN for image classification",
            assets=[],
            inputs=[],
            outputs=[],
        )
        model_projection_config = ModelProjectionConfig(
            name=name,
            category=category,
            version=model_version_config,
        )

        assert model_projection_config.user_tags is None

    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in ModelProjectionConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {"name", "category", "version"}
