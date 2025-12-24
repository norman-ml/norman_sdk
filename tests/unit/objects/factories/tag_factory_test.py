import pytest
from unittest.mock import patch, PropertyMock

from pydantic import ValidationError

from norman_objects.shared.models.model_tag import ModelTag

from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.factories.tag_factory import TagFactory
from norman.managers.authentication_manager import AuthenticationManager


@pytest.fixture
def mock_account_id():
    with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock:
        mock.return_value = "account-123"
        yield mock


@pytest.mark.usefixtures("mock_account_id")
class TestTagFactory:

    def test_create_returns_model_tag_with_defaults(self) -> None:
        config = ModelTagConfig(name="production")

        created_tag = TagFactory.create(config)

        assert isinstance(created_tag, ModelTag)
        assert created_tag.name == "production"
        assert created_tag.id == "0"
        assert created_tag.model_id == "0"

    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        config = ModelTagConfig(name="experimental")

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = "account-456"

            created_tag = TagFactory.create(config)

            assert created_tag.account_id == "account-456"

    def test_create_with_none_account_id_raises_validation_error(self) -> None:
        config = ModelTagConfig(name="invalid-tag")

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = None

            with pytest.raises(ValidationError):
                TagFactory.create(config)
