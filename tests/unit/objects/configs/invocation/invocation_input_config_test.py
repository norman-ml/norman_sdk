import pytest

from norman_objects.shared.inputs.input_source import InputSource

from norman.objects.configs.invocation.invocation_input_config import InvocationInputConfig


@pytest.mark.unit
class TestInvocationInputConfig:
    @pytest.mark.config
    def test_create_with_all_fields(self) -> None:
        display_title = "text_prompt"
        data = "What is the meaning of life?"

        invocation_input_config = InvocationInputConfig(
            display_title=display_title,
            data=data,
            source=InputSource.Primitive,
        )

        assert invocation_input_config.display_title == display_title
        assert invocation_input_config.data == data
        assert invocation_input_config.source == InputSource.Primitive

    @pytest.mark.config
    def test_create_without_optional_fields(self) -> None:
        display_title = "text_input"
        data = "Hello world"

        invocation_input_config = InvocationInputConfig(
            display_title=display_title,
            data=data,
        )

        assert invocation_input_config.source is None

    @pytest.mark.config
    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in InvocationInputConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {"display_title", "data"}
