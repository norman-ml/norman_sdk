import pytest
from unittest.mock import patch

from pydantic import ValidationError

from norman_objects.shared.inputs.input_source import InputSource

from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.objects.factories.invocation_config_factory import InvocationConfigFactory


INPUT_SOURCE_RESOLVER_PATH = "norman.objects.factories.invocation_config_factory.InputSourceResolver"


@pytest.fixture
def mock_input_source_resolver():
    with patch(INPUT_SOURCE_RESOLVER_PATH) as mock:
        mock.resolve.return_value = InputSource.Primitive
        yield mock


@pytest.mark.usefixtures("mock_input_source_resolver")
class TestInvocationConfigFactory:

    def test_create_returns_invocation_config_instance(self) -> None:
        invocation_dict = {"model_name": "text-classifier", "inputs": [{"display_title": "text_input", "data": "sample text"}]}

        created_config = InvocationConfigFactory.create(invocation_dict)

        assert isinstance(created_config, InvocationConfig)
        assert created_config.model_name == "text-classifier"
        assert len(created_config.inputs) == 1

    def test_create_with_outputs(self) -> None:
        invocation_dict = {"model_name": "text-generator", "inputs": [{"display_title": "prompt", "data": "Write a story"}], "outputs": [{"display_title": "generated_text", "data": "placeholder"}]}

        created_config = InvocationConfigFactory.create(invocation_dict)

        assert created_config.outputs is not None
        assert len(created_config.outputs) == 1

    def test_create_without_outputs_defaults_to_none(self) -> None:
        invocation_dict = {"model_name": "classifier", "inputs": [{"display_title": "input", "data": "text"}]}

        created_config = InvocationConfigFactory.create(invocation_dict)

        assert created_config.outputs is None

    def test_create_calls_resolver_when_source_not_provided(self) -> None:
        invocation_dict = {"model_name": "model", "inputs": [{"display_title": "input", "data": "text data"}]}

        with patch(INPUT_SOURCE_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive

            created_config = InvocationConfigFactory.create(invocation_dict)

            mock_resolver.resolve.assert_called_once_with("text data")
            assert created_config.inputs[0].source == InputSource.Primitive

    def test_create_does_not_call_resolver_when_source_provided(self) -> None:
        invocation_dict = {"model_name": "model", "inputs": [{"display_title": "input", "data": "/path/to/file", "source": "File"}]}

        with patch(INPUT_SOURCE_RESOLVER_PATH) as mock_resolver:
            created_config = InvocationConfigFactory.create(invocation_dict)

            mock_resolver.resolve.assert_not_called()
            assert created_config.inputs[0].source == InputSource.File

    def test_create_with_missing_model_name_raises_validation_error(self) -> None:
        invocation_dict = {"inputs": [{"display_title": "input", "data": "data"}]}

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(invocation_dict)

    def test_create_with_missing_inputs_raises_validation_error(self) -> None:
        invocation_dict = {"model_name": "incomplete-model"}

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(invocation_dict)
