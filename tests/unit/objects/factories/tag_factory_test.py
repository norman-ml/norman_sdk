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


@pytest.fixture
def sample_tag_config() -> ModelTagConfig:
    return ModelTagConfig(name="machine-learning")


@pytest.mark.usefixtures("mock_account_id")
class TestTagFactory:

    # --- Return Type and Basic Fields ---

    def test_create_returns_model_tag_instance(self, sample_tag_config: ModelTagConfig) -> None:
        created_tag = TagFactory.create(sample_tag_config)

        assert isinstance(created_tag, ModelTag)

    def test_create_sets_name_from_config(self) -> None:
        production_tag_config = ModelTagConfig(name="production")

        created_tag = TagFactory.create(production_tag_config)

        assert created_tag.name == "production"

    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        experimental_tag_config = ModelTagConfig(name="experimental")

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = "account-456"

            created_tag = TagFactory.create(experimental_tag_config)

            assert created_tag.account_id == "account-456"

    # --- Default Values ---

    def test_create_sets_default_id(self, sample_tag_config: ModelTagConfig) -> None:
        created_tag = TagFactory.create(sample_tag_config)

        assert created_tag.id == "0"

    def test_create_sets_default_model_id(self, sample_tag_config: ModelTagConfig) -> None:
        created_tag = TagFactory.create(sample_tag_config)

        assert created_tag.model_id == "0"

    # --- Name Variations ---

    def test_create_preserves_tag_name_with_special_characters(self) -> None:
        versioned_tag_config = ModelTagConfig(name="pytorch_v2.1-cuda")

        created_tag = TagFactory.create(versioned_tag_config)

        assert created_tag.name == "pytorch_v2.1-cuda"

    def test_create_preserves_tag_name_with_spaces(self) -> None:
        spaced_tag_config = ModelTagConfig(name="natural language processing")

        created_tag = TagFactory.create(spaced_tag_config)

        assert created_tag.name == "natural language processing"

    def test_create_preserves_empty_tag_name(self) -> None:
        empty_tag_config = ModelTagConfig(name="")

        created_tag = TagFactory.create(empty_tag_config)

        assert created_tag.name == ""

    # --- Account ID Edge Cases ---

    def test_create_with_none_account_id_raises_validation_error(self) -> None:
        invalid_account_tag_config = ModelTagConfig(name="invalid-tag")

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = None

            with pytest.raises(ValidationError):
                TagFactory.create(invalid_account_tag_config)

    def test_create_with_empty_account_id(self) -> None:
        empty_account_tag_config = ModelTagConfig(name="empty-account-tag")

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = ""

            created_tag = TagFactory.create(empty_account_tag_config)

            assert created_tag.account_id == ""

    # --- Multiple Calls ---

    def test_create_multiple_tags_returns_independent_results(self) -> None:
        pytorch_tag_config = ModelTagConfig(name="pytorch")
        tensorflow_tag_config = ModelTagConfig(name="tensorflow")

        pytorch_tag = TagFactory.create(pytorch_tag_config)
        tensorflow_tag = TagFactory.create(tensorflow_tag_config)

        assert pytorch_tag.name == "pytorch"
        assert tensorflow_tag.name == "tensorflow"
        assert pytorch_tag.account_id == tensorflow_tag.account_id

    def test_create_uses_current_account_id_at_call_time(self) -> None:
        tag_config = ModelTagConfig(name="dynamic-account-tag")

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = "first-account"
            first_tag = TagFactory.create(tag_config)

            mock_account.return_value = "second-account"
            second_tag = TagFactory.create(tag_config)

            assert first_tag.account_id == "first-account"
            assert second_tag.account_id == "second-account"