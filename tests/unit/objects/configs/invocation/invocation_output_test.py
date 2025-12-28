import pytest

from norman.objects.configs.invocation.consume_mode import ConsumeMode
from norman.objects.configs.invocation.invocation_output_config import InvocationOutputConfig


@pytest.mark.unit
@pytest.mark.config
class TestInvocationOutputConfig:
    def test_create_with_all_fields(self) -> None:
        display_title = "generated_text"
        data = "placeholder"

        invocation_output = InvocationOutputConfig(
            display_title=display_title,
            data=data,
            consume_mode=ConsumeMode.Stream,
        )

        assert invocation_output.display_title == display_title
        assert invocation_output.data == data
        assert invocation_output.consume_mode == ConsumeMode.Stream

    def test_create_without_optional_fields(self) -> None:
        display_title = "classification_result"
        data = b"output bytes"

        invocation_output = InvocationOutputConfig(
            display_title=display_title,
            data=data,
        )

        assert invocation_output.consume_mode is None

    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in InvocationOutputConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {"display_title", "data"}
