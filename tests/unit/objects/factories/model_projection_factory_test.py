from unittest.mock import patch, PropertyMock, MagicMock

import pytest
from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.models.model_tag import ModelTag
from norman_objects.shared.models.model_version import ModelVersion

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.factories.model_projection_factory import ModelProjectionFactory

MODEL_VERSION_FACTORY_PATH = "norman.objects.factories.model_projection_factory.ModelVersionFactory"
TAG_FACTORY_PATH = "norman.objects.factories.model_projection_factory.TagFactory"


@pytest.fixture
def mock_account_id():
    with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock:
        mock.return_value = "account-123"
        yield mock


@pytest.fixture
def mock_version_factory():
    with patch(MODEL_VERSION_FACTORY_PATH) as mock:
        mock_version = MagicMock(spec=ModelVersion)
        mock_version.id = "0"
        mock_version.label = "v1.0"
        mock.create.return_value = mock_version
        yield mock


@pytest.fixture
def mock_tag_factory():
    with patch(TAG_FACTORY_PATH) as mock:
        yield mock


@pytest.fixture
def sample_version_config() -> ModelVersionConfig:
    return ModelVersionConfig(label="v1.0", short_description="Test model", long_description="A test model", assets=[], inputs=[], outputs=[])


@pytest.fixture
def sample_projection_config(sample_version_config: ModelVersionConfig) -> ModelProjectionConfig:
    return ModelProjectionConfig(name="image-classifier", category="computer-vision", version=sample_version_config, user_tags=None)


@pytest.mark.usefixtures("mock_account_id", "mock_version_factory", "mock_tag_factory")
class TestModelProjectionFactory:

    def test_create_returns_model_projection_instance(self, sample_projection_config: ModelProjectionConfig) -> None:
        created_projection = ModelProjectionFactory.create(sample_projection_config)

        assert isinstance(created_projection, ModelProjection)
        assert created_projection.name == "image-classifier"
        assert created_projection.category == "computer-vision"
        assert created_projection.id == "0"
        assert created_projection.invocation_count == 0

    def test_create_sets_account_id_from_authentication_manager(self, sample_projection_config: ModelProjectionConfig) -> None:
        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = "account-456"

            created_projection = ModelProjectionFactory.create(sample_projection_config)

            assert created_projection.account_id == "account-456"

    def test_create_calls_version_factory(self, sample_version_config: ModelVersionConfig) -> None:
        config = ModelProjectionConfig(name="model", category="category", version=sample_version_config, user_tags=None)

        with patch(MODEL_VERSION_FACTORY_PATH) as mock_factory:
            mock_version = MagicMock(spec=ModelVersion)
            mock_factory.create.return_value = mock_version

            created_projection = ModelProjectionFactory.create(config)

            mock_factory.create.assert_called_once_with(sample_version_config)
            assert created_projection.version == mock_version

    def test_create_with_none_user_tags_returns_empty_list(self, sample_projection_config: ModelProjectionConfig) -> None:
        with patch(TAG_FACTORY_PATH) as mock_factory:
            created_projection = ModelProjectionFactory.create(sample_projection_config)

            assert created_projection.user_tags == []
            mock_factory.create.assert_not_called()

    def test_create_with_user_tags_calls_tag_factory(self, sample_version_config: ModelVersionConfig) -> None:
        tag_config = ModelTagConfig(name="production")
        config = ModelProjectionConfig(name="model", category="category", version=sample_version_config, user_tags=[tag_config])

        with patch(TAG_FACTORY_PATH) as mock_factory:
            mock_tag = MagicMock(spec=ModelTag)
            mock_tag.name = "production"
            mock_factory.create.return_value = mock_tag

            created_projection = ModelProjectionFactory.create(config)

            mock_factory.create.assert_called_once_with(tag_config)
            assert len(created_projection.user_tags) == 1
