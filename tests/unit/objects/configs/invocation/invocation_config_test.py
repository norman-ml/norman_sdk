from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.objects.configs.invocation.invocation_input_config import InvocationInputConfig
from norman.objects.configs.invocation.invocation_output_config import InvocationOutputConfig


class TestInvocationConfig:

    def test_create_with_all_fields(self) -> None:
        input_config = InvocationInputConfig(display_title="text_prompt", data="Hello world")
        output_config = InvocationOutputConfig(display_title="generated_text", data="placeholder")
        config = InvocationConfig(model_name="sentiment-analyzer", inputs=[input_config], outputs=[output_config])

        assert config.model_name == "sentiment-analyzer"
        assert len(config.inputs) == 1
        assert config.inputs[0].display_title == "text_prompt"
        assert len(config.outputs) == 1
        assert config.outputs[0].display_title == "generated_text"

    def test_create_without_optional_fields(self) -> None:
        input_config = InvocationInputConfig(display_title="audio_input", data=b"audio bytes")
        config = InvocationConfig(model_name="speech-recognizer", inputs=[input_config])

        assert config.outputs is None

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in InvocationConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"model_name", "inputs"}