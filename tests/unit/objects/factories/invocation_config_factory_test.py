import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from pydantic import ValidationError

from norman_objects.shared.inputs.input_source import InputSource

from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.objects.factories.invocation_config_factory import InvocationConfigFactory


class TestInvocationConfigFactoryCreate:
    def test_create_returns_invocation_config_instance(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "test data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert isinstance(result, InvocationConfig)

    def test_create_sets_model_name(self):
        config_dict = {
            "model_name": "my-model",
            "inputs": [{"display_title": "input1", "data": "test data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert result.model_name == "my-model"

    def test_create_parses_inputs(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [
                {"display_title": "input1", "data": "data1"},
                {"display_title": "input2", "data": "data2"}
            ]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert len(result.inputs) == 2
        assert result.inputs[0].display_title == "input1"
        assert result.inputs[1].display_title == "input2"

    # --- Source Resolution ---

    def test_create_resolves_source_when_none(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "test data", "source": None}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert result.inputs[0].source == InputSource.Primitive
        mock_resolver.resolve.assert_called_once_with("test data")

    def test_create_resolves_source_when_not_provided(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "test data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Link
            result = InvocationConfigFactory.create(config_dict)

        assert result.inputs[0].source == InputSource.Link
        mock_resolver.resolve.assert_called_once()

    def test_create_preserves_source_when_provided(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "test data", "source": "File"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert result.inputs[0].source == InputSource.File
        mock_resolver.resolve.assert_not_called()

    def test_create_resolves_each_input_independently(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [
                {"display_title": "input1", "data": "string data"},
                {"display_title": "input2", "data": "https://example.com"},
                {"display_title": "input3", "data": "more data", "source": "Stream"}
            ]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.side_effect = [InputSource.Primitive, InputSource.Link]
            result = InvocationConfigFactory.create(config_dict)

        assert result.inputs[0].source == InputSource.Primitive
        assert result.inputs[1].source == InputSource.Link
        assert result.inputs[2].source == InputSource.Stream
        assert mock_resolver.resolve.call_count == 2

    def test_create_with_bytes_data(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": b"binary data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert result.inputs[0].data == b"binary data"
        mock_resolver.resolve.assert_called_once_with(b"binary data")

    def test_create_with_string_data(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "string data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert result.inputs[0].data == "string data"

    def test_create_with_no_outputs(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "test data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert result.outputs is None

    def test_create_with_outputs(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "test data"}],
            "outputs": [{"display_title": "output1", "data": "output data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            result = InvocationConfigFactory.create(config_dict)

        assert result.outputs is not None
        assert len(result.outputs) == 1
        assert result.outputs[0].display_title == "output1"

    def test_create_with_missing_model_name_raises_validation_error(self):
        config_dict = {
            "inputs": [{"display_title": "input1", "data": "test data"}]
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(config_dict)

    def test_create_with_missing_inputs_raises_validation_error(self):
        config_dict = {
            "model_name": "test-model"
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(config_dict)

    def test_create_with_empty_inputs_list(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": []
        }

        result = InvocationConfigFactory.create(config_dict)

        assert result.inputs == []

    def test_create_with_invalid_input_raises_validation_error(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1"}]  # Missing 'data' field
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(config_dict)

    def test_create_with_none_data_raises_validation_error(self):
        """None is not a valid data type - Pydantic rejects before resolver runs"""
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": None}]
        }

        with pytest.raises(ValidationError):
            InvocationConfigFactory.create(config_dict)

    def test_create_propagates_resolver_value_error(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "valid string data"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.side_effect = ValueError("Custom resolver error")

            with pytest.raises(ValueError, match="Custom resolver error"):
                InvocationConfigFactory.create(config_dict)

    def test_create_propagates_resolver_file_not_found_error(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [{"display_title": "input1", "data": "/nonexistent/path"}]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.side_effect = FileNotFoundError("No file exists at the specified location")

            with pytest.raises(FileNotFoundError):
                InvocationConfigFactory.create(config_dict)


class TestInvocationConfigFactoryInputSourceResolution:
    def test_resolver_called_with_correct_data(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [
                {"display_title": "input1", "data": "data-one"},
                {"display_title": "input2", "data": "data-two"}
            ]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.return_value = InputSource.Primitive
            InvocationConfigFactory.create(config_dict)

            assert mock_resolver.resolve.call_count == 2
            mock_resolver.resolve.assert_any_call("data-one")
            mock_resolver.resolve.assert_any_call("data-two")

    def test_resolver_not_called_when_all_sources_provided(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [
                {"display_title": "input1", "data": "data1", "source": "File"},
                {"display_title": "input2", "data": "data2", "source": "Link"}
            ]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            InvocationConfigFactory.create(config_dict)

            mock_resolver.resolve.assert_not_called()

    def test_resolver_uses_return_value_for_each_input(self):
        config_dict = {
            "model_name": "test-model",
            "inputs": [
                {"display_title": "input1", "data": "primitive"},
                {"display_title": "input2", "data": "https://example.com"},
                {"display_title": "input3", "data": b"bytes"}
            ]
        }

        with patch('norman.objects.factories.invocation_config_factory.InputSourceResolver') as mock_resolver:
            mock_resolver.resolve.side_effect = [
                InputSource.Primitive,
                InputSource.Link,
                InputSource.Stream
            ]
            result = InvocationConfigFactory.create(config_dict)

        assert result.inputs[0].source == InputSource.Primitive
        assert result.inputs[1].source == InputSource.Link
        assert result.inputs[2].source == InputSource.Stream
        