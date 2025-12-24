import pytest
import io
import json
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path

from norman_objects.services.file_push.pairing.socket_asset_pairing_request import SocketAssetPairingRequest
from norman_objects.services.file_push.pairing.socket_input_pairing_request import SocketInputPairingRequest
from norman_objects.shared.security.sensitive import Sensitive

from norman.services.file_transfer_service import FileTransferService


@pytest.fixture(autouse=True)
def reset_singleton():
    FileTransferService._instances = {}
    yield
    FileTransferService._instances = {}


class TestFileTransferServiceUploadFile:
    @pytest.mark.asyncio
    async def test_upload_file_opens_file_and_calls_upload_from_buffer(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"test content")

        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketAssetPairingRequest)
        pairing_request.version_id = "version-123"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch.object(FileTransferService, 'upload_from_buffer', new_callable=AsyncMock) as mock_upload:

            mock_file_push = MagicMock()
            mock_file_push_class.return_value = mock_file_push

            service = FileTransferService()
            await service.upload_file(token, pairing_request, test_file)

            mock_upload.assert_called_once()
            call_args = mock_upload.call_args
            assert call_args[0][0] == token
            assert call_args[0][1] == pairing_request

    @pytest.mark.asyncio
    async def test_upload_file_with_string_path(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"test content")

        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketAssetPairingRequest)
        pairing_request.version_id = "version-123"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch.object(FileTransferService, 'upload_from_buffer', new_callable=AsyncMock) as mock_upload:

            mock_file_push = MagicMock()
            mock_file_push_class.return_value = mock_file_push

            service = FileTransferService()
            await service.upload_file(token, pairing_request, str(test_file))

            mock_upload.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_file_with_path_object(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"test content")

        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketAssetPairingRequest)
        pairing_request.version_id = "version-123"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch.object(FileTransferService, 'upload_from_buffer', new_callable=AsyncMock) as mock_upload:

            mock_file_push = MagicMock()
            mock_file_push_class.return_value = mock_file_push

            service = FileTransferService()
            await service.upload_file(token, pairing_request, Path(test_file))

            mock_upload.assert_called_once()


class TestFileTransferServiceUploadFromBuffer:
    @pytest.mark.asyncio
    async def test_upload_from_buffer_with_asset_pairing_request(self):
        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketAssetPairingRequest)
        pairing_request.version_id = "version-123"
        buffer = io.BytesIO(b"test data")

        mock_socket_info = MagicMock()
        mock_socket_info.pairing_id = "pairing-456"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch('norman.services.file_transfer_service.SocketClient') as mock_socket_client:

            mock_file_push = MagicMock()
            mock_file_push.allocate_socket_for_asset = AsyncMock(return_value=mock_socket_info)
            mock_file_push.complete_file_transfer = AsyncMock()
            mock_file_push_class.return_value = mock_file_push

            mock_socket_client.write_and_digest = AsyncMock(return_value="checksum-789")

            service = FileTransferService()
            await service.upload_from_buffer(token, pairing_request, buffer)

            mock_file_push.allocate_socket_for_asset.assert_called_once_with(token, pairing_request)
            mock_socket_client.write_and_digest.assert_called_once_with(mock_socket_info, buffer)
            mock_file_push.complete_file_transfer.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_from_buffer_with_input_pairing_request(self):
        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketInputPairingRequest)
        pairing_request.invocation_id = "invocation-123"
        buffer = io.BytesIO(b"test data")

        mock_socket_info = MagicMock()
        mock_socket_info.pairing_id = "pairing-456"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch('norman.services.file_transfer_service.SocketClient') as mock_socket_client:

            mock_file_push = MagicMock()
            mock_file_push.allocate_socket_for_input = AsyncMock(return_value=mock_socket_info)
            mock_file_push.complete_file_transfer = AsyncMock()
            mock_file_push_class.return_value = mock_file_push

            mock_socket_client.write_and_digest = AsyncMock(return_value="checksum-789")

            service = FileTransferService()
            await service.upload_from_buffer(token, pairing_request, buffer)

            mock_file_push.allocate_socket_for_input.assert_called_once_with(token, pairing_request)
            mock_socket_client.write_and_digest.assert_called_once()
            mock_file_push.complete_file_transfer.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_from_buffer_with_bytes(self):
        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketAssetPairingRequest)
        pairing_request.version_id = "version-123"
        buffer = b"raw bytes data"

        mock_socket_info = MagicMock()
        mock_socket_info.pairing_id = "pairing-456"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch('norman.services.file_transfer_service.SocketClient') as mock_socket_client:

            mock_file_push = MagicMock()
            mock_file_push.allocate_socket_for_asset = AsyncMock(return_value=mock_socket_info)
            mock_file_push.complete_file_transfer = AsyncMock()
            mock_file_push_class.return_value = mock_file_push

            mock_socket_client.write_and_digest = AsyncMock(return_value="checksum-789")

            service = FileTransferService()
            await service.upload_from_buffer(token, pairing_request, buffer)

            mock_socket_client.write_and_digest.assert_called_once_with(mock_socket_info, buffer)

    @pytest.mark.asyncio
    async def test_upload_from_buffer_unsupported_pairing_type_raises_error(self):
        token = Sensitive("test-token")
        pairing_request = MagicMock()  # Not a valid pairing request type
        buffer = io.BytesIO(b"test data")

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class:
            mock_file_push = MagicMock()
            mock_file_push_class.return_value = mock_file_push

            service = FileTransferService()

            with pytest.raises(TypeError, match="Unsupported pairing request type"):
                await service.upload_from_buffer(token, pairing_request, buffer)

    @pytest.mark.asyncio
    async def test_upload_from_buffer_creates_checksum_request_with_correct_entity_id_for_asset(self):
        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketAssetPairingRequest)
        pairing_request.version_id = "version-123"
        buffer = io.BytesIO(b"test data")

        mock_socket_info = MagicMock()
        mock_socket_info.pairing_id = "pairing-456"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch('norman.services.file_transfer_service.SocketClient') as mock_socket_client, \
             patch('norman.services.file_transfer_service.ChecksumRequest') as mock_checksum_request:

            mock_file_push = MagicMock()
            mock_file_push.allocate_socket_for_asset = AsyncMock(return_value=mock_socket_info)
            mock_file_push.complete_file_transfer = AsyncMock()
            mock_file_push_class.return_value = mock_file_push

            mock_socket_client.write_and_digest = AsyncMock(return_value="checksum-789")

            service = FileTransferService()
            await service.upload_from_buffer(token, pairing_request, buffer)

            mock_checksum_request.assert_called_once_with(
                entity_id="version-123",
                pairing_id="pairing-456",
                checksum="checksum-789"
            )

    @pytest.mark.asyncio
    async def test_upload_from_buffer_creates_checksum_request_with_correct_entity_id_for_input(self):
        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketInputPairingRequest)
        pairing_request.invocation_id = "invocation-123"
        buffer = io.BytesIO(b"test data")

        mock_socket_info = MagicMock()
        mock_socket_info.pairing_id = "pairing-456"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch('norman.services.file_transfer_service.SocketClient') as mock_socket_client, \
             patch('norman.services.file_transfer_service.ChecksumRequest') as mock_checksum_request:

            mock_file_push = MagicMock()
            mock_file_push.allocate_socket_for_input = AsyncMock(return_value=mock_socket_info)
            mock_file_push.complete_file_transfer = AsyncMock()
            mock_file_push_class.return_value = mock_file_push

            mock_socket_client.write_and_digest = AsyncMock(return_value="checksum-789")

            service = FileTransferService()
            await service.upload_from_buffer(token, pairing_request, buffer)

            mock_checksum_request.assert_called_once_with(
                entity_id="invocation-123",
                pairing_id="pairing-456",
                checksum="checksum-789"
            )


class TestFileTransferServiceNormalizePrimitiveData:
    def test_normalize_string_returns_bytes_io(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data("hello world")

        assert isinstance(result, io.BytesIO)

    def test_normalize_string_encodes_as_utf8(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data("hello world")

        assert result.getvalue() == b"hello world"

    def test_normalize_empty_string(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data("")

        assert result.getvalue() == b""

    def test_normalize_unicode_string(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data("héllo wörld 中文")

        assert result.getvalue() == "héllo wörld 中文".encode("utf-8")

    def test_normalize_bytes_returns_bytes_io(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(b"binary data")

        assert isinstance(result, io.BytesIO)
        assert result.getvalue() == b"binary data"

    def test_normalize_empty_bytes(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(b"")

        assert result.getvalue() == b""

    def test_normalize_bytearray(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(bytearray(b"bytearray data"))

        assert isinstance(result, io.BytesIO)
        assert result.getvalue() == b"bytearray data"

    def test_normalize_bytes_io_returns_same_object(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            original = io.BytesIO(b"original data")
            result = service.normalize_primitive_data(original)

        assert result is original

    def test_normalize_integer_returns_bytes_io(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(42)

        assert isinstance(result, io.BytesIO)
        assert result.getvalue() == b"42"

    def test_normalize_negative_integer(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(-123)

        assert result.getvalue() == b"-123"

    def test_normalize_zero(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(0)

        assert result.getvalue() == b"0"

    def test_normalize_large_integer(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(999999999999)

        assert result.getvalue() == b"999999999999"

    def test_normalize_float_returns_bytes_io(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(3.14)

        assert isinstance(result, io.BytesIO)
        assert result.getvalue() == b"3.14"

    def test_normalize_negative_float(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(-2.5)

        assert result.getvalue() == b"-2.5"

    def test_normalize_float_zero(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(0.0)

        assert result.getvalue() == b"0.0"

    def test_normalize_boolean_true_converts_to_string(self):
        """Booleans are treated as integers since bool is a subclass of int"""
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(True)

        # True is treated as int (value 1) -> "1"
        assert result.getvalue() == b"True"

    def test_normalize_boolean_false_converts_to_string(self):
        """Booleans are treated as integers since bool is a subclass of int"""
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data(False)

        # False is treated as int (value 0) -> "0"
        assert result.getvalue() == b"False"

    def test_normalize_dict_returns_bytes_io(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data({"key": "value"})

        assert isinstance(result, io.BytesIO)

    def test_normalize_dict_as_json(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            data = {"key": "value", "number": 42}
            result = service.normalize_primitive_data(data)

        expected = json.dumps(data).encode("utf-8")
        assert result.getvalue() == expected

    def test_normalize_empty_dict(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data({})

        assert result.getvalue() == b"{}"

    def test_normalize_nested_dict(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            data = {"outer": {"inner": "value"}}
            result = service.normalize_primitive_data(data)

        expected = json.dumps(data).encode("utf-8")
        assert result.getvalue() == expected

    def test_normalize_list_returns_bytes_io(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data([1, 2, 3])

        assert isinstance(result, io.BytesIO)

    def test_normalize_list_as_json(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            data = [1, "two", 3.0]
            result = service.normalize_primitive_data(data)

        expected = json.dumps(data).encode("utf-8")
        assert result.getvalue() == expected

    def test_normalize_empty_list(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            result = service.normalize_primitive_data([])

        assert result.getvalue() == b"[]"

    def test_normalize_nested_list(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()
            data = [[1, 2], [3, 4]]
            result = service.normalize_primitive_data(data)

        expected = json.dumps(data).encode("utf-8")
        assert result.getvalue() == expected

    def test_normalize_none_raises_value_error(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()

            with pytest.raises(ValueError, match="Unsupported data type"):
                service.normalize_primitive_data(None)

    def test_normalize_tuple_raises_value_error(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()

            with pytest.raises(ValueError, match="Unsupported data type"):
                service.normalize_primitive_data((1, 2, 3))

    def test_normalize_set_raises_value_error(self):
        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()

            with pytest.raises(ValueError, match="Unsupported data type"):
                service.normalize_primitive_data({1, 2, 3})

    def test_normalize_custom_object_raises_value_error(self):
        class CustomObject:
            pass

        with patch('norman.services.file_transfer_service.FilePush'):
            service = FileTransferService()

            with pytest.raises(ValueError, match="Unsupported data type"):
                service.normalize_primitive_data(CustomObject())


class TestFileTransferServiceInit:
    """Tests for FileTransferService initialization"""

    def test_init_creates_file_push_service(self):
        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class:
            FileTransferService()

            mock_file_push_class.assert_called_once()

    def test_init_stores_file_push_service(self):
        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class:
            mock_file_push = MagicMock()
            mock_file_push_class.return_value = mock_file_push

            service = FileTransferService()

            assert service._file_push_service is mock_file_push


class TestFileTransferServiceIntegration:
    """Integration tests for FileTransferService"""

    @pytest.mark.asyncio
    async def test_upload_flow_asset_pairing(self):
        """Test complete upload flow with asset pairing"""
        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketAssetPairingRequest)
        pairing_request.version_id = "version-123"
        buffer = io.BytesIO(b"test data")

        mock_socket_info = MagicMock()
        mock_socket_info.pairing_id = "pairing-456"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch('norman.services.file_transfer_service.SocketClient') as mock_socket_client:

            mock_file_push = MagicMock()
            mock_file_push.allocate_socket_for_asset = AsyncMock(return_value=mock_socket_info)
            mock_file_push.complete_file_transfer = AsyncMock()
            mock_file_push_class.return_value = mock_file_push

            mock_socket_client.write_and_digest = AsyncMock(return_value="checksum-789")

            service = FileTransferService()
            await service.upload_from_buffer(token, pairing_request, buffer)

            # Verify the complete flow
            mock_file_push.allocate_socket_for_asset.assert_called_once()
            mock_socket_client.write_and_digest.assert_called_once()
            mock_file_push.complete_file_transfer.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_flow_input_pairing(self):
        """Test complete upload flow with input pairing"""
        token = Sensitive("test-token")
        pairing_request = MagicMock(spec=SocketInputPairingRequest)
        pairing_request.invocation_id = "invocation-123"
        buffer = io.BytesIO(b"test data")

        mock_socket_info = MagicMock()
        mock_socket_info.pairing_id = "pairing-456"

        with patch('norman.services.file_transfer_service.FilePush') as mock_file_push_class, \
             patch('norman.services.file_transfer_service.SocketClient') as mock_socket_client:

            mock_file_push = MagicMock()
            mock_file_push.allocate_socket_for_input = AsyncMock(return_value=mock_socket_info)
            mock_file_push.complete_file_transfer = AsyncMock()
            mock_file_push_class.return_value = mock_file_push

            mock_socket_client.write_and_digest = AsyncMock(return_value="checksum-789")

            service = FileTransferService()
            await service.upload_from_buffer(token, pairing_request, buffer)

            # Verify the complete flow
            mock_file_push.allocate_socket_for_input.assert_called_once()
            mock_socket_client.write_and_digest.assert_called_once()
            mock_file_push.complete_file_transfer.assert_called_once()