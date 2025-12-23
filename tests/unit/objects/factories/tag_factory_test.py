import pytest
from unittest.mock import patch, PropertyMock

from pydantic import ValidationError

from norman_objects.shared.models.model_tag import ModelTag

from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.factories.tag_factory import TagFactory
from norman.managers.authentication_manager import AuthenticationManager


class TestTagFactoryCreate:
    def test_create_returns_model_tag_instance(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = TagFactory.create(config)

        assert isinstance(result, ModelTag)

    def test_create_sets_name_from_config(self):
        config = ModelTagConfig(name="my-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = TagFactory.create(config)

        assert result.name == "my-tag"

    def test_create_sets_account_id_from_authentication_manager(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-456"
            result = TagFactory.create(config)

        assert result.account_id == "account-456"


    def test_create_sets_default_id(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = TagFactory.create(config)

        assert result.id == "0"

    def test_create_sets_default_model_id(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = TagFactory.create(config)

        assert result.model_id == "0"

    def test_create_preserves_tag_name_with_special_characters(self):
        config = ModelTagConfig(name="my-tag_v1.0")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = TagFactory.create(config)

        assert result.name == "my-tag_v1.0"

    def test_create_preserves_tag_name_with_spaces(self):
        config = ModelTagConfig(name="tag with spaces")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = TagFactory.create(config)

        assert result.name == "tag with spaces"

    def test_create_preserves_empty_tag_name(self):
        config = ModelTagConfig(name="")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = TagFactory.create(config)

        assert result.name == ""


    def test_create_with_none_account_id_raises_validation_error(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = None

            with pytest.raises(ValidationError):
                TagFactory.create(config)

    def test_create_with_empty_account_id(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = ""
            result = TagFactory.create(config)

        assert result.account_id == ""


    def test_create_multiple_tags_with_different_names(self):
        config1 = ModelTagConfig(name="tag-one")
        config2 = ModelTagConfig(name="tag-two")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result1 = TagFactory.create(config1)
            result2 = TagFactory.create(config2)

        assert result1.name == "tag-one"
        assert result2.name == "tag-two"
        assert result1.account_id == result2.account_id

    def test_create_reads_account_id_property(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "read-account-id"
            TagFactory.create(config)

            mock_account_id.assert_called()

    def test_create_uses_current_account_id_at_call_time(self):
        config = ModelTagConfig(name="test-tag")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "first-account"
            result1 = TagFactory.create(config)

            mock_account_id.return_value = "second-account"
            result2 = TagFactory.create(config)

        assert result1.account_id == "first-account"
        assert result2.account_id == "second-account"