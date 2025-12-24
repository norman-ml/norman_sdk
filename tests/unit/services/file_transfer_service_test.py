import pytest
import io
import json
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path

from norman_objects.services.file_push.pairing.socket_asset_pairing_request import SocketAssetPairingRequest
from norman_objects.services.file_push.pairing.socket_input_pairing_request import SocketInputPairingRequest
from norman_objects.shared.security.sensitive import Sensitive

from norman.services.file_transfer_service import FileTransferService

FILE_PUSH_PATH = "norman.services.file_transfer_service.FilePush"
SOCKET_CLIENT_PATH = "norman.services.file_transfer_service.SocketClient"

@pytest.fixture(autouse=True)
def reset_singleton():
    FileTransferService._instances = {}
    yield
    FileTransferService._instances = {}

@pytest.fixture
def mock_file_push():
    with patch(FILE_PUSH_PATH) as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_socket_client():
    with patch(SOCKET_CLIENT_PATH) as mock:
        mock.write_and_digest = AsyncMock(return_value="checksum-abc123")
        yield mock

@pytest.fixture
def authentication_token() -> Sensitive:
    return Sensitive("api-token-xyz789")

@pytest.fixture
def mock_socket_info() -> MagicMock:
    socket_info = MagicMock()
    socket_info.pairing_id = "pairing-ghi789"
    return socket_info

@pytest.mark.usefixtures("reset_singleton", "mock_file_push", "mock_socket_client")
class TestFileTransferService:

    @pytest.mark.asyncio
    async def test_upload_file_opens_file_and_delegates_to_upload_from_buffer(self, tmp_path: Path, authentication_token: Sensitive) -> None:
        model_weights_file = tmp_path / "model_weights.pt"
        model_weights_file.write_bytes(b"binary model data")
        asset_pairing = MagicMock(spec=SocketAssetPairingRequest)
        asset_pairing.version_id = "version-abc123"

        with patch.object(FileTransferService, "upload_from_buffer", new_callable=AsyncMock) as mock_upload:
            service = FileTransferService()
            await service.upload_file(authentication_token, asset_pairing, model_weights_file)

            mock_upload.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_from_buffer_with_asset_pairing(self, mock_file_push: MagicMock, mock_socket_client: MagicMock, mock_socket_info: MagicMock, authentication_token: Sensitive) -> None:
        mock_file_push.allocate_socket_for_asset = AsyncMock(return_value=mock_socket_info)
        mock_file_push.complete_file_transfer = AsyncMock()
        asset_pairing = MagicMock(spec=SocketAssetPairingRequest)
        asset_pairing.version_id = "version-abc123"

        service = FileTransferService()
        await service.upload_from_buffer(authentication_token, asset_pairing, io.BytesIO(b"data"))

        mock_file_push.allocate_socket_for_asset.assert_called_once()
        mock_file_push.complete_file_transfer.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_from_buffer_with_input_pairing(self, mock_file_push: MagicMock, mock_socket_client: MagicMock, mock_socket_info: MagicMock, authentication_token: Sensitive) -> None:
        mock_file_push.allocate_socket_for_input = AsyncMock(return_value=mock_socket_info)
        mock_file_push.complete_file_transfer = AsyncMock()
        input_pairing = MagicMock(spec=SocketInputPairingRequest)
        input_pairing.invocation_id = "invocation-def456"

        service = FileTransferService()
        await service.upload_from_buffer(authentication_token, input_pairing, io.BytesIO(b"data"))

        mock_file_push.allocate_socket_for_input.assert_called_once()
        mock_file_push.complete_file_transfer.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_from_buffer_unsupported_pairing_type_raises_error(self, mock_file_push: MagicMock, authentication_token: Sensitive) -> None:
        service = FileTransferService()

        with pytest.raises(TypeError, match="Unsupported pairing request type"):
            await service.upload_from_buffer(authentication_token, MagicMock(), io.BytesIO(b"data"))

    def test_normalize_string_returns_utf8_bytes_io(self, mock_file_push: MagicMock) -> None:
        service = FileTransferService()
        result = service.normalize_primitive_data("Hello, model!")

        assert isinstance(result, io.BytesIO)
        assert result.getvalue() == b"Hello, model!"

    def test_normalize_bytes_returns_bytes_io(self, mock_file_push: MagicMock) -> None:
        service = FileTransferService()

        result = service.normalize_primitive_data(b"\x00\x01\x02\xff")

        assert result.getvalue() == b"\x00\x01\x02\xff"

    def test_normalize_bytes_io_returns_same_object(self, mock_file_push: MagicMock) -> None:
        service = FileTransferService()
        original_buffer = io.BytesIO(b"original")

        result = service.normalize_primitive_data(original_buffer)

        assert result is original_buffer

    def test_normalize_integer_returns_string_bytes_io(self, mock_file_push: MagicMock) -> None:
        service = FileTransferService()

        result = service.normalize_primitive_data(42)

        assert result.getvalue() == b"42"

    def test_normalize_dict_returns_json_bytes_io(self, mock_file_push: MagicMock) -> None:
        service = FileTransferService()
        config = {"hidden_size": 768}

        result = service.normalize_primitive_data(config)

        assert result.getvalue() == json.dumps(config).encode("utf-8")

    @pytest.mark.parametrize("unsupported_input", [None, (1, 2), {1, 2}])
    def test_normalize_unsupported_types_raise_value_error(self, mock_file_push: MagicMock, unsupported_input) -> None:
        service = FileTransferService()

        with pytest.raises(ValueError, match="Unsupported data type"):
            service.normalize_primitive_data(unsupported_input)