import pytest

from norman.objects.configs.model.model_tag_config import ModelTagConfig


@pytest.mark.unit
@pytest.mark.config
class TestModelTagConfig:
    def test_create_with_all_fields(self) -> None:
        name = "production"

        model_tag = ModelTagConfig(name=name)

        assert model_tag.name == name

    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in ModelTagConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {"name"}
