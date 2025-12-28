import pytest

from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.objects.configs.invocation.invocation_input_config import InvocationInputConfig
from norman.objects.configs.invocation.invocation_output_config import InvocationOutputConfig


@pytest.mark.unit
@pytest.mark.config
class TestInvocationConfig:
    MODEL_NAME_SENTIMENT = "sentiment-analyzer"
    MODEL_NAME_SPEECH = "speech-recognizer"
    DISPLAY_TITLE_TEXT_PROMPT = "text_prompt"
    DISPLAY_TITLE_GENERATED_TEXT = "generated_text"
    DISPLAY_TITLE_AUDIO_INPUT = "audio_input"
    INPUT_DATA_TEXT = "Hello world"
    INPUT_DATA_AUDIO = b"audio bytes"
    OUTPUT_DATA_PLACEHOLDER = "placeholder"

    def test_create_with_all_fields(self) -> None:
        invocation_input_config = InvocationInputConfig(
            display_title=self.DISPLAY_TITLE_TEXT_PROMPT,
            data=self.INPUT_DATA_TEXT,
        )
        invocation_output_config = InvocationOutputConfig(
            display_title=self.DISPLAY_TITLE_GENERATED_TEXT,
            data=self.OUTPUT_DATA_PLACEHOLDER,
        )
        invocation_config = InvocationConfig(
            model_name=self.MODEL_NAME_SENTIMENT,
            inputs=[invocation_input_config],
            outputs=[invocation_output_config],
        )

        assert invocation_config.model_name == self.MODEL_NAME_SENTIMENT
        assert len(invocation_config.inputs) == 1
        assert invocation_config.inputs[0].display_title == self.DISPLAY_TITLE_TEXT_PROMPT
        assert len(invocation_config.outputs) == 1
        assert invocation_config.outputs[0].display_title == self.DISPLAY_TITLE_GENERATED_TEXT

    def test_create_without_optional_fields(self) -> None:
        invocation_input_config = InvocationInputConfig(
            display_title=self.DISPLAY_TITLE_AUDIO_INPUT,
            data=self.INPUT_DATA_AUDIO,
        )
        invocation_config = InvocationConfig(
            model_name=self.MODEL_NAME_SPEECH,
            inputs=[invocation_input_config],
        )

        assert invocation_config.outputs is None

    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in InvocationConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {"model_name", "inputs"}
