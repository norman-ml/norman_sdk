import pytest
import io
from typing import Generator
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path

from norman_objects.services.file_push.pairing.socket_asset_pairing_request import SocketAssetPairingRequest
from norman_objects.services.file_push.pairing.socket_input_pairing_request import SocketInputPairingRequest
from norman_objects.shared.security.sensitive import Sensitive

from norman.services.file_transfer_service import FileTransferService


@pytest.fixture(scope="function")
def socket_client_mock() -> Generator[MagicMock, None, None]:
    with patch("norman.services.file_transfer_service.SocketClient") as mock:
        mock.write_and_digest = AsyncMock(return_value="checksum-abc123")
        yield mock


@pytest.fixture(scope="function")
def sensitive_token() -> Sensitive:
    return Sensitive("api-token-xyz789")


@pytest.fixture(scope="function")
def socket_info_mock() -> MagicMock:
    socket_info = MagicMock()
    socket_info.pairing_id = "pairing-ghi789"
    return socket_info


@pytest.mark.usefixtures("file_transfer_service_reset", "file_push_mock", "socket_client_mock")
class TestFileTransferServiceUpload:
    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_file_opens_file_and_delegates_to_upload_from_buffer(self, tmp_path: Path, sensitive_token: Sensitive) -> None:
        model_weights_file = tmp_path / "model_weights.pt"
        model_weights_file.write_bytes(b"binary model data")
        asset_pairing = MagicMock(spec=SocketAssetPairingRequest)
        asset_pairing.version_id = "version-abc123"

        with patch.object(FileTransferService, "upload_from_buffer", new_callable=AsyncMock) as upload_from_buffer_mock:
            file_transfer_service = FileTransferService()
            await file_transfer_service.upload_file(sensitive_token, asset_pairing, model_weights_file)

            upload_from_buffer_mock.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_from_buffer_with_asset_pairing(self, file_push_mock: MagicMock, socket_client_mock: MagicMock, socket_info_mock: MagicMock, sensitive_token: Sensitive) -> None:
        file_push_mock.allocate_socket_for_asset = AsyncMock(return_value=socket_info_mock)
        file_push_mock.complete_file_transfer = AsyncMock()
        asset_pairing = MagicMock(spec=SocketAssetPairingRequest)
        asset_pairing.version_id = "version-abc123"

        file_transfer_service = FileTransferService()
        await file_transfer_service.upload_from_buffer(sensitive_token, asset_pairing, io.BytesIO(b"data"))

        file_push_mock.allocate_socket_for_asset.assert_called_once()
        file_push_mock.complete_file_transfer.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_from_buffer_with_input_pairing(self, file_push_mock: MagicMock, socket_client_mock: MagicMock, socket_info_mock: MagicMock, sensitive_token: Sensitive) -> None:
        file_push_mock.allocate_socket_for_input = AsyncMock(return_value=socket_info_mock)
        file_push_mock.complete_file_transfer = AsyncMock()
        input_pairing = MagicMock(spec=SocketInputPairingRequest)
        input_pairing.invocation_id = "invocation-def456"

        file_transfer_service = FileTransferService()
        await file_transfer_service.upload_from_buffer(sensitive_token, input_pairing, io.BytesIO(b"data"))

        file_push_mock.allocate_socket_for_input.assert_called_once()
        file_push_mock.complete_file_transfer.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_from_buffer_unsupported_pairing_type_raises_error(self, file_push_mock: MagicMock, sensitive_token: Sensitive) -> None:
        file_transfer_service = FileTransferService()

        with pytest.raises(TypeError, match="Unsupported pairing request type"):
            await file_transfer_service.upload_from_buffer(sensitive_token, MagicMock(), io.BytesIO(b"data"))
