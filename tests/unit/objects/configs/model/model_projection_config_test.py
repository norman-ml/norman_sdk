from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig


class TestModelProjectionConfig:

    def test_create_with_all_fields(self) -> None:
        version = ModelVersionConfig(label="v1.0", short_description="Text classifier", long_description="A model for classifying text", assets=[], inputs=[], outputs=[])
        tag = ModelTagConfig(name="production")
        config = ModelProjectionConfig(name="sentiment-analyzer", category="natural-language-processing", version=version, user_tags=[tag])

        assert config.name == "sentiment-analyzer"
        assert config.category == "natural-language-processing"
        assert config.version.label == "v1.0"
        assert len(config.user_tags) == 1
        assert config.user_tags[0].name == "production"

    def test_create_without_optional_fields(self) -> None:
        version = ModelVersionConfig(label="v1.0", short_description="Image classifier", long_description="A CNN for image classification", assets=[], inputs=[], outputs=[])
        config = ModelProjectionConfig(name="image-classifier", category="computer-vision", version=version)

        assert config.user_tags is None

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in ModelProjectionConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"name", "category", "version"}
