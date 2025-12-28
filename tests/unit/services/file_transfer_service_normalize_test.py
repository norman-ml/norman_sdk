import pytest
import io
import json
from typing import Any

from norman.services.file_transfer_service import FileTransferService


@pytest.mark.usefixtures("file_transfer_service_reset", "file_push_mock")
class TestFileTransferServiceNormalize:
    @pytest.mark.unit
    def test_normalize_string_returns_utf8_bytes_io(self) -> None:
        file_transfer_service = FileTransferService()

        result = file_transfer_service.normalize_primitive_data("Hello, model!")

        assert isinstance(result, io.BytesIO)
        assert result.getvalue() == b"Hello, model!"

    @pytest.mark.unit
    def test_normalize_bytes_returns_bytes_io(self) -> None:
        file_transfer_service = FileTransferService()

        result = file_transfer_service.normalize_primitive_data(b"\x00\x01\x02\xff")

        assert result.getvalue() == b"\x00\x01\x02\xff"

    @pytest.mark.unit
    def test_normalize_bytes_io_returns_same_object(self) -> None:
        file_transfer_service = FileTransferService()
        original_buffer = io.BytesIO(b"original")

        result = file_transfer_service.normalize_primitive_data(original_buffer)

        assert result is original_buffer

    @pytest.mark.unit
    def test_normalize_integer_returns_string_bytes_io(self) -> None:
        file_transfer_service = FileTransferService()

        result = file_transfer_service.normalize_primitive_data(42)

        assert result.getvalue() == b"42"

    @pytest.mark.unit
    def test_normalize_dict_returns_json_bytes_io(self) -> None:
        file_transfer_service = FileTransferService()
        config = {"hidden_size": 768}

        result = file_transfer_service.normalize_primitive_data(config)

        assert result.getvalue() == json.dumps(config).encode("utf-8")

    @pytest.mark.unit
    @pytest.mark.parametrize("unsupported_input", [None, (1, 2), {1, 2}])
    def test_normalize_unsupported_types_raise_value_error(self, unsupported_input: Any) -> None:
        file_transfer_service = FileTransferService()

        with pytest.raises(ValueError, match="Unsupported data type"):
            file_transfer_service.normalize_primitive_data(unsupported_input)
