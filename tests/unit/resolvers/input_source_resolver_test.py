import pytest
from pathlib import Path
from io import BytesIO

from norman_objects.shared.inputs.input_source import InputSource

from norman.resolvers.input_source_resolver import InputSourceResolver


class TestInputSourceResolver:

    def test_resolve_none_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Input data cannot be None"):
            InputSourceResolver.resolve(None)

    def test_resolve_existing_path_returns_file(self, tmp_path: Path) -> None:
        model_weights_file = tmp_path / "model_weights.pt"
        model_weights_file.write_bytes(b"binary weights data")

        resolved_source = InputSourceResolver.resolve(model_weights_file)

        assert resolved_source == InputSource.File

    def test_resolve_nonexistent_path_raises_file_not_found(self, tmp_path: Path) -> None:
        nonexistent_model_path = tmp_path / "missing_model.pt"

        with pytest.raises(FileNotFoundError, match="No file exists"):
            InputSourceResolver.resolve(nonexistent_model_path)

    def test_resolve_https_url_returns_link(self) -> None:
        model_endpoint_url = "https://api.example.com/models/classifier"

        resolved_source = InputSourceResolver.resolve(model_endpoint_url)

        assert resolved_source == InputSource.Link

    def test_resolve_plain_string_returns_primitive(self) -> None:
        prompt_text = "Summarize the following document"

        resolved_source = InputSourceResolver.resolve(prompt_text)

        assert resolved_source == InputSource.Primitive

    def test_resolve_bytes_returns_primitive(self) -> None:
        audio_bytes = b"\x00\x01\x02\x03\xff\xfe"

        resolved_source = InputSourceResolver.resolve(audio_bytes)

        assert resolved_source == InputSource.Primitive

    def test_resolve_dict_returns_primitive(self) -> None:
        model_config = {"hidden_size": 768, "num_layers": 12}

        resolved_source = InputSourceResolver.resolve(model_config)

        assert resolved_source == InputSource.Primitive

    def test_resolve_bytes_io_returns_stream(self) -> None:
        audio_stream = BytesIO(b"audio waveform data")

        resolved_source = InputSourceResolver.resolve(audio_stream)

        assert resolved_source == InputSource.Stream

    def test_resolve_async_stream_returns_stream(self) -> None:
        class AsyncModelOutputStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            @staticmethod
            async def read():
                return b"inference result"

        async_stream = AsyncModelOutputStream()

        resolved_source = InputSourceResolver.resolve(async_stream)

        assert resolved_source == InputSource.Stream

    def test_resolve_file_handle_returns_stream(self, tmp_path: Path) -> None:
        data_file = tmp_path / "training_data.bin"
        data_file.write_bytes(b"training samples")

        with open(data_file, "rb") as file_handle:
            resolved_source = InputSourceResolver.resolve(file_handle)

            assert resolved_source == InputSource.Stream