import pytest
from pydantic import ValidationError

from norman_objects.shared.inputs.input_source import InputSource

from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.objects.factories.invocation_config_factory import InvocationConfigFactory
from tests.constants import SAMPLE_INPUTS_DIR, SAMPLE_INPUT_TXT


@pytest.mark.unit
@pytest.mark.factory
class TestInvocationConfigFactory:
    def test_create_returns_invocation_config_instance(self) -> None:
        model_name = "text-classifier"
        display_title = "text_input"
        data = "sample text"

        invocation_dict = {
            "model_name": model_name,
            "inputs": [{"display_title": display_title, "data": data}],
        }

        invocation_config = InvocationConfigFactory.create(invocation_dict)

        assert isinstance(invocation_config, InvocationConfig)
        assert invocation_config.model_name == model_name
        assert len(invocation_config.inputs) == 1

    def test_create_with_outputs(self) -> None:
        invocation_dict = {
            "model_name": "text-generator",
            "inputs": [{"display_title": "prompt", "data": "Write a story"}],
            "outputs": [{"display_title": "generated_text", "data": "placeholder"}],
        }

        invocation_config = InvocationConfigFactory.create(invocation_dict)

        assert invocation_config.outputs is not None
        assert len(invocation_config.outputs) == 1

    def test_create_without_outputs_defaults_to_none(self) -> None:
        invocation_dict = {
            "model_name": "classifier",
            "inputs": [{"display_title": "input", "data": "text"}],
        }

        invocation_config = InvocationConfigFactory.create(invocation_dict)

        assert invocation_config.outputs is None

    def test_create_resolves_primitive_source_from_string_data(self) -> None:
        invocation_dict = {
            "model_name": "model",
            "inputs": [{"display_title": "input", "data": "text data"}],
        }

        invocation_config = InvocationConfigFactory.create(invocation_dict)

        assert invocation_config.inputs[0].source == InputSource.Primitive

    def test_create_resolves_primitive_source_from_bytes_data(self) -> None:
        invocation_dict = {
            "model_name": "model",
            "inputs": [{"display_title": "input", "data": b"binary data"}],
        }

        invocation_config = InvocationConfigFactory.create(invocation_dict)

        assert invocation_config.inputs[0].source == InputSource.Primitive

    def test_create_resolves_file_source_from_path(self) -> None:
        file_path = str(SAMPLE_INPUTS_DIR / SAMPLE_INPUT_TXT)

        invocation_dict = {
            "model_name": "model",
            "inputs": [{"display_title": "input", "data": file_path}],
        }

        invocation_config = InvocationConfigFactory.create(invocation_dict)

        assert invocation_config.inputs[0].source == InputSource.File

    def test_create_preserves_explicit_source(self) -> None:
        invocation_dict = {
            "model_name": "model",
            "inputs": [{"display_title": "input", "data": "some data", "source": "Primitive"}],
        }

        invocation_config = InvocationConfigFactory.create(invocation_dict)

        assert invocation_config.inputs[0].source == InputSource.Primitive

    def test_create_with_missing_model_name_raises_validation_error(self) -> None:
        invocation_dict = {
            "inputs": [{"display_title": "input", "data": "data"}],
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(invocation_dict)

    def test_create_with_missing_inputs_raises_validation_error(self) -> None:
        invocation_dict = {"model_name": "incomplete-model"}

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(invocation_dict)
