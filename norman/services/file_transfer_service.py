import io
import json
from pathlib import Path
from typing import Any, Union

import aiofiles
from norman_core.clients.socket_client import SocketClient
from norman_core.services.file_push.file_push import FilePush
from norman_objects.services.file_push.checksum.checksum_request import ChecksumRequest
from norman_objects.services.file_push.pairing.socket_asset_pairing_request import SocketAssetPairingRequest
from norman_objects.services.file_push.pairing.socket_input_pairing_request import SocketInputPairingRequest
from norman_objects.shared.security.sensitive import Sensitive
from norman_utils.singleton import Singleton


class FileTransferService(metaclass=Singleton):
    def __init__(self) -> None:
        self._file_push_service = FilePush()

    async def upload_file(self, token: Sensitive[str], pairing_request: Union[SocketAssetPairingRequest, SocketInputPairingRequest], path: Union[str, Path]) -> None:
        async with aiofiles.open(path, mode="rb") as file:
            await self.upload_from_buffer(token, pairing_request, file)

    async def upload_from_buffer(self, token: Sensitive[str], pairing_request: Union[SocketAssetPairingRequest, SocketInputPairingRequest], buffer: Union[bytes, io.BytesIO]) -> None:
        if isinstance(pairing_request, SocketAssetPairingRequest):
            entity_id = pairing_request.version_id
            socket_info = await self._file_push_service.allocate_socket_for_asset(token, pairing_request)
        elif isinstance(pairing_request, SocketInputPairingRequest):
            entity_id = pairing_request.invocation_id
            socket_info = await self._file_push_service.allocate_socket_for_input(token, pairing_request)
        else:
            raise TypeError("Unsupported pairing request type")

        checksum = await SocketClient.write_and_digest(socket_info, buffer)
        checksum_request = ChecksumRequest(entity_id=entity_id, pairing_id=socket_info.pairing_id, checksum=checksum)
        await self._file_push_service.complete_file_transfer(token, checksum_request)

    def normalize_primitive_data(self, data: Any) -> io.BytesIO:
        if isinstance(data, str):
            return io.BytesIO(data.encode("utf8"))

        elif isinstance(data, (bytes, bytearray)):
            return io.BytesIO(data)

        elif isinstance(data, io.BytesIO):
            return data

        elif isinstance(data, (int, float)):
            return io.BytesIO(str(data).encode("utf8"))

        elif isinstance(data, (dict, list)):
            json_str = json.dumps(data)
            return io.BytesIO(json_str.encode("utf8"))

        else:
            raise ValueError(f"Unsupported data type: {type(data)}. Cannot convert to BytesIO.")
