import pytest
from unittest.mock import PropertyMock

from pydantic import ValidationError

from norman_objects.shared.models.model_tag import ModelTag

from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.factories.tag_factory import TagFactory
from tests.constants import DEFAULT_ID, DEFAULT_MODEL_ID, TEST_ACCOUNT_ID


@pytest.mark.unit
@pytest.mark.factory
@pytest.mark.usefixtures("account_id_mock")
class TestTagFactory:
    def test_create_returns_model_tag_with_defaults(self) -> None:
        name = "production"

        model_tag_config = ModelTagConfig(name=name)

        model_tag = TagFactory.create(model_tag_config)

        assert isinstance(model_tag, ModelTag)
        assert model_tag.name == name
        assert model_tag.id == DEFAULT_ID
        assert model_tag.model_id == DEFAULT_MODEL_ID

    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        model_tag_config = ModelTagConfig(name="experimental")

        model_tag = TagFactory.create(model_tag_config)

        assert model_tag.account_id == TEST_ACCOUNT_ID

    def test_create_with_none_account_id_raises_validation_error(self,account_id_mock: PropertyMock,) -> None:
        account_id_mock.return_value = None
        model_tag_config = ModelTagConfig(name="invalid-tag")

        with pytest.raises(ValidationError):
            TagFactory.create(model_tag_config)
