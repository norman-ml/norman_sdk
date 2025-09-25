import asyncio
import io
from typing import Any, Union

import aiofiles
from norman_core.clients.http_client import HttpClient
from norman_core.clients.socket_client import SocketClient
from norman_core.services.file_pull.file_pull import FilePull
from norman_core.services.file_push.file_push import FilePush
from norman_core.services.persist import Persist
from norman_core.services.retrieve.retrieve import Retrieve
from norman_objects.services.file_pull.requests.input_download_request import InputDownloadRequest
from norman_objects.services.file_push.checksum.checksum_request import ChecksumRequest
from norman_objects.services.file_push.pairing.socket_input_pairing_request import SocketInputPairingRequest
from norman_objects.shared.invocation_signatures.invocation_signature import InvocationSignature
from norman_objects.shared.invocations.invocation import Invocation
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag import StatusFlag
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue
from norman_utils_external.streaming_utils import AsyncBufferedReader, BufferedReader

from norman.helpers.get_buffer_size import get_buffer_size
from norman.objects.configs.invocation_config import InvocationConfig


class InvocationManager:
    @staticmethod
    async def create_invocation_in_database(http_client: HttpClient, token: Sensitive[str], invocation_config: InvocationConfig):
        invocations = await Persist.invocations.create_invocations_by_model_names(http_client, token, {invocation_config["model_name"]: 1})
        return invocations[0]

    @staticmethod
    async def upload_inputs(http_client: HttpClient, token: Sensitive[str], invocation: Invocation, invocation_config: InvocationConfig):
        _tasks = []

        for input in invocation.inputs:
            input_config = invocation_config["inputs"][input.display_title]
            input_source = input_config["source"]
            input_data = input_config["data"]
            if input_source == "Primitive":
                _tasks.append(InvocationManager._upload_primitive(http_client, token, input, input_data))
            elif input_source == "Path":
                _tasks.append(InvocationManager._upload_file(http_client, token, input, input_data))
            elif input_source == "Stream":
                _tasks.append(InvocationManager._upload_buffer(http_client, token, input, input_data))
            else:
                _tasks.append(InvocationManager._upload_link(http_client, token, input, input_data))

        await asyncio.gather(*_tasks)

    @staticmethod
    async def wait_for_flags(http_client, token, invocation: Invocation):
        while True:
            invocation_constraints = QueryConstraints.equals("Invocation_Flags", "Entity_ID", invocation.id)
            results = await Persist.invocation_flags.get_invocation_status_flags(http_client, token, invocation_constraints)
            if len(results) == 0:
                raise ValueError(f"Invocation {invocation.id} has no flags")

            all_flags: list[StatusFlag] = []
            for key in results:
                all_flags.extend(results[key])

            any_failed = any(flag.flag_value == StatusFlagValue.Error for flag in all_flags)
            all_finished = all(flag.flag_value == StatusFlagValue.Finished for flag in all_flags)

            if any_failed:
                raise ValueError(f"Invocation {invocation.id} has failed")
            if all_finished:
                break

            await asyncio.sleep(1)

    @staticmethod
    async def get_results(http_client: HttpClient, token: Sensitive[str], invocation: Invocation) -> dict[str, bytearray]:
        output_tasks = []
        for output in invocation.outputs:
            task = InvocationManager.get_output_results(http_client, token, invocation, output.id)
            output_tasks.append(task)

        results: list[bytearray] = await asyncio.gather(*output_tasks)
        return {
            output.display_title: result
            for output, result in zip(invocation.outputs, results)
        }

    @staticmethod
    async def get_output_results(http_client: HttpClient, token: Sensitive[str], invocation: Invocation, output_id: str) -> bytearray:
        account_id = invocation.account_id
        model_id = invocation.model_id
        invocation_id = invocation.id

        headers, stream = await Retrieve.get_invocation_output(http_client, token, account_id, model_id, invocation_id, output_id)

        results = bytearray()
        async for chunk in stream:
            results.extend(chunk)

        return results

    @staticmethod
    async def _upload_primitive(http_client: HttpClient, token: Sensitive[str], input: InvocationSignature, data: Any):
        buffer = io.BytesIO()
        buffer.write(str(data).encode("utf-8"))
        buffer.seek(0)

        await InvocationManager._upload_buffer(http_client, token, input, buffer)

    @staticmethod
    async def _upload_file(http_client: HttpClient, token: Sensitive[str], input: InvocationSignature, file_path: str):
        async with aiofiles.open(file_path, mode="rb") as file:
            await InvocationManager._upload_buffer(http_client, token, input, file)

    @staticmethod
    async def _upload_buffer(http_client: HttpClient, token: Sensitive[str], input: InvocationSignature, buffer: Union[AsyncBufferedReader, BufferedReader]):
        pairing_request = SocketInputPairingRequest(
            invocation_id=input.invocation_id,
            input_id=input.id,
            account_id=input.account_id,
            model_id=input.model_id,
            file_size_in_bytes=get_buffer_size(buffer)
        )
        request = await FilePush.allocate_socket_for_input(
            http_client, token, pairing_request
        )

        file_checksum = await SocketClient.write_and_digest(request, buffer)

        checksum_request = ChecksumRequest(
            pairing_id=request.pairing_id, checksum=file_checksum
        )
        await FilePush.complete_file_transfer(http_client, token, checksum_request)

    @staticmethod
    async def _upload_link(http_client: HttpClient, token: Sensitive[str], input: InvocationSignature, link: str):
        download_request = InputDownloadRequest(
            signature_id=input.signature_id,
            invocation_id=input.invocation_id,
            input_id=input.id,
            account_id=input.account_id,
            model_id=input.model_id,
            links=[link]
        )
        response = await FilePull.submit_input_links(http_client, token, download_request)
        return response
