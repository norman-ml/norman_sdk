from norman.objects.configs.model.model_tag_config import ModelTagConfig


class TestModelTagConfig:

    def test_create_with_all_fields(self) -> None:
        config = ModelTagConfig(name="production")

        assert config.name == "production"

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in ModelTagConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"name"}
