from norman_objects.shared.inputs.input_source import InputSource

from norman.objects.configs.invocation.invocation_input_config import InvocationInputConfig


class TestInvocationInputConfig:

    def test_create_with_all_fields(self) -> None:
        config = InvocationInputConfig(display_title="audio_input", data=b"binary audio data", source=InputSource.Primitive)

        assert config.display_title == "audio_input"
        assert config.data == b"binary audio data"
        assert config.source == InputSource.Primitive

    def test_create_without_optional_fields(self) -> None:
        config = InvocationInputConfig(display_title="text_prompt", data="What is the meaning of life?")

        assert config.source is None

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in InvocationInputConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"display_title", "data"}
