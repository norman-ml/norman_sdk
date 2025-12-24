from norman.objects.configs.invocation.consume_mode import ConsumeMode
from norman.objects.configs.invocation.invocation_output_config import InvocationOutputConfig


class TestInvocationOutputConfig:

    def test_create_with_all_fields(self) -> None:
        config = InvocationOutputConfig(display_title="generated_text", data="placeholder", consume_mode=ConsumeMode.Stream)

        assert config.display_title == "generated_text"
        assert config.data == "placeholder"
        assert config.consume_mode == ConsumeMode.Stream

    def test_create_without_optional_fields(self) -> None:
        config = InvocationOutputConfig(display_title="classification_result", data=b"output bytes")

        assert config.consume_mode is None

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in InvocationOutputConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"display_title", "data"}
