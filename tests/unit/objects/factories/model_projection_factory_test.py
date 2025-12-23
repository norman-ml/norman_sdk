import pytest
from unittest.mock import patch, PropertyMock, MagicMock

from pydantic import ValidationError

from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.models.model_version import ModelVersion
from norman_objects.shared.models.model_tag import ModelTag

from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.factories.model_projection_factory import ModelProjectionFactory
from norman.managers.authentication_manager import AuthenticationManager


def create_minimal_version_config():
    """Helper to create a minimal valid ModelVersionConfig"""
    return ModelVersionConfig(
        label="v1.0",
        short_description="Test model",
        long_description="A test model for unit testing",
        assets=[],
        inputs=[],
        outputs=[]
    )


def create_mock_model_version():
    """Helper to create a mock ModelVersion"""
    mock_version = MagicMock(spec=ModelVersion)
    mock_version.id = "0"
    mock_version.account_id = "account-123"
    mock_version.label = "v1.0"
    return mock_version


def create_mock_model_tag(name="test-tag"):
    """Helper to create a mock ModelTag"""
    mock_tag = MagicMock(spec=ModelTag)
    mock_tag.id = "0"
    mock_tag.account_id = "account-123"
    mock_tag.name = name
    return mock_tag


class TestModelProjectionFactoryCreate:
    """Tests for ModelProjectionFactory.create() method"""

    # --- Success Cases ---

    def test_create_returns_model_projection_instance(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory, \
             patch('norman.objects.factories.model_projection_factory.TagFactory') as mock_tag_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert isinstance(result, ModelProjection)

    def test_create_sets_name_from_config(self):
        config = ModelProjectionConfig(
            name="my-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.name == "my-model"

    def test_create_sets_category_from_config(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="image-recognition",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.category == "image-recognition"

    def test_create_sets_account_id_from_authentication_manager(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-456"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.account_id == "account-456"

    def test_create_sets_invocation_count_to_zero(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.invocation_count == 0

    # --- Default Values ---

    def test_create_sets_default_id(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.id == "0"

    # --- Category Handling ---

    def test_create_with_none_category_sets_empty_string(self):
        """When category is None, it should be set to empty string"""
        config = ModelProjectionConfig(
            name="test-model",
            category="placeholder",  # Will be overridden
            version=create_minimal_version_config()
        )
        # Manually set category to None after creation to bypass Pydantic
        config.category = None

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.category == ""

    def test_create_preserves_empty_category(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.category == ""

    # --- Version Factory Integration ---

    def test_create_calls_model_version_factory(self):
        version_config = create_minimal_version_config()
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=version_config
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            ModelProjectionFactory.create(config)

            mock_version_factory.create.assert_called_once_with(version_config)

    def test_create_sets_version_from_factory(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        mock_version = create_mock_model_version()
        mock_version.label = "v2.0-beta"

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = mock_version

            result = ModelProjectionFactory.create(config)

        assert result.version == mock_version

    # --- User Tags Handling ---

    def test_create_with_no_user_tags(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config(),
            user_tags=None
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory, \
             patch('norman.objects.factories.model_projection_factory.TagFactory') as mock_tag_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.user_tags == []
        mock_tag_factory.create.assert_not_called()

    def test_create_with_empty_user_tags_list(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config(),
            user_tags=[]
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory, \
             patch('norman.objects.factories.model_projection_factory.TagFactory') as mock_tag_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.user_tags == []
        mock_tag_factory.create.assert_not_called()

    def test_create_with_single_user_tag(self):
        tag_config = ModelTagConfig(name="my-tag")
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config(),
            user_tags=[tag_config]
        )

        mock_tag = create_mock_model_tag("my-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory, \
             patch('norman.objects.factories.model_projection_factory.TagFactory') as mock_tag_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()
            mock_tag_factory.create.return_value = mock_tag

            result = ModelProjectionFactory.create(config)

        assert len(result.user_tags) == 1
        assert result.user_tags[0] == mock_tag
        mock_tag_factory.create.assert_called_once_with(tag_config)

    def test_create_with_multiple_user_tags(self):
        tag_configs = [
            ModelTagConfig(name="tag-one"),
            ModelTagConfig(name="tag-two"),
            ModelTagConfig(name="tag-three")
        ]
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config(),
            user_tags=tag_configs
        )

        mock_tags = [
            create_mock_model_tag("tag-one"),
            create_mock_model_tag("tag-two"),
            create_mock_model_tag("tag-three")
        ]

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory, \
             patch('norman.objects.factories.model_projection_factory.TagFactory') as mock_tag_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()
            mock_tag_factory.create.side_effect = mock_tags

            result = ModelProjectionFactory.create(config)

        assert len(result.user_tags) == 3
        assert mock_tag_factory.create.call_count == 3

    def test_create_calls_tag_factory_for_each_tag(self):
        tag_configs = [
            ModelTagConfig(name="tag-one"),
            ModelTagConfig(name="tag-two")
        ]
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config(),
            user_tags=tag_configs
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory, \
             patch('norman.objects.factories.model_projection_factory.TagFactory') as mock_tag_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()
            mock_tag_factory.create.side_effect = [
                create_mock_model_tag("tag-one"),
                create_mock_model_tag("tag-two")
            ]

            ModelProjectionFactory.create(config)

            mock_tag_factory.create.assert_any_call(tag_configs[0])
            mock_tag_factory.create.assert_any_call(tag_configs[1])

    # --- Name Variations ---

    def test_create_preserves_name_with_special_characters(self):
        config = ModelProjectionConfig(
            name="my-model_v1.0",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.name == "my-model_v1.0"

    def test_create_preserves_name_with_spaces(self):
        config = ModelProjectionConfig(
            name="My Test Model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result = ModelProjectionFactory.create(config)

        assert result.name == "My Test Model"

    # --- Account ID Edge Cases ---

    def test_create_with_none_account_id_raises_validation_error(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = None
            mock_version_factory.create.return_value = create_mock_model_version()

            with pytest.raises(ValidationError):
                ModelProjectionFactory.create(config)

    # --- Multiple Calls ---

    def test_create_multiple_projections_with_different_names(self):
        config1 = ModelProjectionConfig(
            name="model-one",
            category="classification",
            version=create_minimal_version_config()
        )
        config2 = ModelProjectionConfig(
            name="model-two",
            category="detection",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            result1 = ModelProjectionFactory.create(config1)
            result2 = ModelProjectionFactory.create(config2)

        assert result1.name == "model-one"
        assert result2.name == "model-two"
        assert result1.category == "classification"
        assert result2.category == "detection"


class TestModelProjectionFactoryIntegration:
    """Tests verifying factory integration with dependencies"""

    def test_create_reads_account_id_property(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()

            ModelProjectionFactory.create(config)

            mock_account_id.assert_called()

    def test_create_uses_current_account_id_at_call_time(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "first-account"
            mock_version_factory.create.return_value = create_mock_model_version()

            result1 = ModelProjectionFactory.create(config)

            mock_account_id.return_value = "second-account"
            result2 = ModelProjectionFactory.create(config)

        assert result1.account_id == "first-account"
        assert result2.account_id == "second-account"

    def test_version_factory_error_propagates(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config()
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.side_effect = ValueError("Version factory error")

            with pytest.raises(ValueError, match="Version factory error"):
                ModelProjectionFactory.create(config)

    def test_tag_factory_error_propagates(self):
        config = ModelProjectionConfig(
            name="test-model",
            category="classification",
            version=create_minimal_version_config(),
            user_tags=[ModelTagConfig(name="test-tag")]
        )

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id, \
             patch('norman.objects.factories.model_projection_factory.ModelVersionFactory') as mock_version_factory, \
             patch('norman.objects.factories.model_projection_factory.TagFactory') as mock_tag_factory:
            mock_account_id.return_value = "account-123"
            mock_version_factory.create.return_value = create_mock_model_version()
            mock_tag_factory.create.side_effect = ValidationError.from_exception_data("test", [])

            with pytest.raises(ValidationError):
                ModelProjectionFactory.create(config)