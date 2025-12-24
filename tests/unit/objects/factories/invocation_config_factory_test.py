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


@pytest.fixture
def minimal_invocation_dict() -> dict:
    return {
        "model_name": "text-classifier",
        "inputs": [{"display_title": "text_input", "data": "sample text for classification"}]
    }


@pytest.mark.usefixtures("mock_input_source_resolver")
class TestInvocationConfigFactory:
    # --- Return Type and Basic Fields ---

    def test_create_returns_invocation_config_instance(self, minimal_invocation_dict: dict) -> None:
        created_config = InvocationConfigFactory.create(minimal_invocation_dict)

        assert isinstance(created_config, InvocationConfig)

    def test_create_sets_model_name_from_dict(self) -> None:
        sentiment_model_dict = {
            "model_name": "sentiment-analyzer",
            "inputs": [{"display_title": "review_text", "data": "This product is amazing!"}]
        }

        created_config = InvocationConfigFactory.create(sentiment_model_dict)

        assert created_config.model_name == "sentiment-analyzer"

    def test_create_parses_multiple_inputs(self) -> None:
        multi_input_dict = {
            "model_name": "image-captioner",
            "inputs": [
                {"display_title": "image_data", "data": b"binary image data"},
                {"display_title": "language_preference", "data": "english"}
            ]
        }

        created_config = InvocationConfigFactory.create(multi_input_dict)

        assert len(created_config.inputs) == 2
        assert created_config.inputs[0].display_title == "image_data"
        assert created_config.inputs[1].display_title == "language_preference"

    def test_create_with_bytes_data(self) -> None:
        binary_input_dict = {
            "model_name": "audio-transcriber",
            "inputs": [{"display_title": "audio_file", "data": b"\x00\x01\x02\x03"}]
        }

        created_config = InvocationConfigFactory.create(binary_input_dict)

        assert created_config.inputs[0].data == b"\x00\x01\x02\x03"

    def test_create_with_string_data(self) -> None:
        text_input_dict = {
            "model_name": "translation-model",
            "inputs": [{"display_title": "source_text", "data": "Hello, world!"}]
        }

        created_config = InvocationConfigFactory.create(text_input_dict)

        assert created_config.inputs[0].data == "Hello, world!"

    def test_create_with_empty_inputs_list(self) -> None:
        empty_inputs_dict = {
            "model_name": "no-input-model",
            "inputs": []
        }

        created_config = InvocationConfigFactory.create(empty_inputs_dict)

        assert created_config.inputs == []

    # --- Outputs Handling ---

    def test_create_with_no_outputs_defaults_to_none(self, minimal_invocation_dict: dict) -> None:
        created_config = InvocationConfigFactory.create(minimal_invocation_dict)

        assert created_config.outputs is None

    def test_create_with_single_output(self) -> None:
        single_output_dict = {
            "model_name": "text-generator",
            "inputs": [{"display_title": "prompt", "data": "Write a story about"}],
            "outputs": [{"display_title": "generated_text", "data": "placeholder"}]
        }

        created_config = InvocationConfigFactory.create(single_output_dict)

        assert created_config.outputs is not None
        assert len(created_config.outputs) == 1
        assert created_config.outputs[0].display_title == "generated_text"

    def test_create_with_multiple_outputs(self) -> None:
        multi_output_dict = {
            "model_name": "object-detector",
            "inputs": [{"display_title": "image", "data": b"image bytes"}],
            "outputs": [
                {"display_title": "bounding_boxes", "data": "boxes placeholder"},
                {"display_title": "class_labels", "data": "labels placeholder"}
            ]
        }

        created_config = InvocationConfigFactory.create(multi_output_dict)

        assert created_config.outputs is not None
        assert len(created_config.outputs) == 2
        assert created_config.outputs[0].display_title == "bounding_boxes"
        assert created_config.outputs[1].display_title == "class_labels"

    # --- Source Resolution Integration ---

    def test_create_calls_resolver_when_source_not_provided(self) -> None:
        no_source_dict = {
            "model_name": "embedding-model",
            "inputs": [{"display_title": "document", "data": "sample document text"}]
        }

        with patch(INPUT_SOURCE_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive

            created_config = InvocationConfigFactory.create(no_source_dict)

            mock_resolver.resolve.assert_called_once_with("sample document text")
            assert created_config.inputs[0].source == InputSource.Primitive

    def test_create_does_not_call_resolver_when_source_provided(self) -> None:
        explicit_source_dict = {
            "model_name": "file-processor",
            "inputs": [{"display_title": "input_file", "data": "/path/to/file.txt", "source": "File"}]
        }

        with patch(INPUT_SOURCE_RESOLVER_PATH) as mock_resolver:
            created_config = InvocationConfigFactory.create(explicit_source_dict)

            mock_resolver.resolve.assert_not_called()
            assert created_config.inputs[0].source == InputSource.File

    def test_create_calls_resolver_for_each_input_without_source(self) -> None:
        mixed_source_dict = {
            "model_name": "multi-modal-model",
            "inputs": [
                {"display_title": "text_input", "data": "plain text"},
                {"display_title": "file_input", "data": "/data/file.bin", "source": "Stream"},
                {"display_title": "url_input", "data": "https://api.example.com/data"}
            ]
        }

        with patch(INPUT_SOURCE_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.side_effect = [InputSource.Primitive, InputSource.Link]

            created_config = InvocationConfigFactory.create(mixed_source_dict)

            assert mock_resolver.resolve.call_count == 2
            assert created_config.inputs[0].source == InputSource.Primitive
            assert created_config.inputs[1].source == InputSource.Stream
            assert created_config.inputs[2].source == InputSource.Link

    def test_create_propagates_resolver_error(self) -> None:
        error_dict = {
            "model_name": "error-model",
            "inputs": [{"display_title": "problematic_input", "data": "data causing error"}]
        }

        with patch(INPUT_SOURCE_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.side_effect = ValueError("Unable to resolve input source")

            with pytest.raises(ValueError, match="Unable to resolve input source"):
                InvocationConfigFactory.create(error_dict)

    # --- Validation Errors ---

    def test_create_with_missing_model_name_raises_validation_error(self) -> None:
        missing_model_name_dict = {
            "inputs": [{"display_title": "input", "data": "some data"}]
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(missing_model_name_dict)

    def test_create_with_missing_inputs_raises_validation_error(self) -> None:
        missing_inputs_dict = {
            "model_name": "incomplete-model"
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(missing_inputs_dict)

    def test_create_with_missing_input_data_raises_validation_error(self) -> None:
        missing_data_dict = {
            "model_name": "missing-data-model",
            "inputs": [{"display_title": "incomplete_input"}]
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(missing_data_dict)

    def test_create_with_none_data_raises_validation_error(self) -> None:
        null_data_dict = {
            "model_name": "null-data-model",
            "inputs": [{"display_title": "null_input", "data": None}]
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(null_data_dict)