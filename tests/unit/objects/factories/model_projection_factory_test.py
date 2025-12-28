import pytest

from norman_objects.shared.models.model_projection import ModelProjection

from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.factories.model_projection_factory import ModelProjectionFactory
from tests.constants import DEFAULT_ID, TEST_ACCOUNT_ID


@pytest.mark.usefixtures("account_id_mock")
class TestModelProjectionFactory:
    @pytest.mark.unit
    @pytest.mark.factory
    def test_create_returns_model_projection_instance(self) -> None:
        name = "image-classifier"
        category = "computer-vision"

        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test model",
            long_description="A test model",
            assets=[],
            inputs=[],
            outputs=[],
        )
        model_projection_config = ModelProjectionConfig(
            name=name,
            category=category,
            version=model_version_config,
            user_tags=None,
        )

        model_projection = ModelProjectionFactory.create(model_projection_config)

        assert isinstance(model_projection, ModelProjection)
        assert model_projection.name == name
        assert model_projection.category == category
        assert model_projection.id == DEFAULT_ID
        assert model_projection.invocation_count == 0

    @pytest.mark.unit
    @pytest.mark.factory
    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test model",
            long_description="A test model",
            assets=[],
            inputs=[],
            outputs=[],
        )
        model_projection_config = ModelProjectionConfig(
            name="model",
            category="category",
            version=model_version_config,
            user_tags=None,
        )

        model_projection = ModelProjectionFactory.create(model_projection_config)

        assert model_projection.account_id == TEST_ACCOUNT_ID

    @pytest.mark.unit
    @pytest.mark.factory
    def test_create_creates_version_from_config(self) -> None:
        version_label = "v2.0"

        model_version_config = ModelVersionConfig(
            label=version_label,
            short_description="Test model",
            long_description="A test model",
            assets=[],
            inputs=[],
            outputs=[],
        )
        model_projection_config = ModelProjectionConfig(
            name="model",
            category="category",
            version=model_version_config,
            user_tags=None,
        )

        model_projection = ModelProjectionFactory.create(model_projection_config)

        assert model_projection.version.label == version_label

    @pytest.mark.unit
    @pytest.mark.factory
    def test_create_with_none_user_tags_returns_empty_list(self) -> None:
        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test model",
            long_description="A test model",
            assets=[],
            inputs=[],
            outputs=[],
        )
        model_projection_config = ModelProjectionConfig(
            name="model",
            category="category",
            version=model_version_config,
            user_tags=None,
        )

        model_projection = ModelProjectionFactory.create(model_projection_config)

        assert model_projection.user_tags == []

    @pytest.mark.unit
    @pytest.mark.factory
    def test_create_with_user_tags_creates_tags(self) -> None:
        tag_name = "production"

        model_version_config = ModelVersionConfig(
            label="v1.0",
            short_description="Test model",
            long_description="A test model",
            assets=[],
            inputs=[],
            outputs=[],
        )
        model_tag_config = ModelTagConfig(name=tag_name)
        model_projection_config = ModelProjectionConfig(
            name="model",
            category="category",
            version=model_version_config,
            user_tags=[model_tag_config],
        )

        model_projection = ModelProjectionFactory.create(model_projection_config)

        assert len(model_projection.user_tags) == 1
        assert model_projection.user_tags[0].name == tag_name
