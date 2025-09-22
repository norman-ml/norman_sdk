import asyncio
import io
import os
from typing import Any, Union, Optional

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

from norman.objects.configs.invocation_config import InvocationConfig
from norman.objects.trackers.invocation_tracker import InvocationTracker, _InvocationStage, _InvocationStatus, InvocationEvent


class InvocationManager:
    def __init__(self, http_client: HttpClient, token: Sensitive[str], invocation_config: InvocationConfig, progress_tracker: Optional[InvocationTracker] = None):
        self.__http_client = http_client
        self.__token = token
        self.invocation_config = invocation_config
        self.invocation: Optional[Invocation] = None

        self._progress_tracker = progress_tracker

    def _update_progress(self, stage: _InvocationStage, status: _InvocationStatus = "Starting", flags: Optional[list[StatusFlag]] = None) -> None:
        if self._progress_tracker is not None:
            invocation_id = ""
            model_id  = ""
            account_id = ""
            if self.invocation is not None:
                invocation_id = self.invocation.id
                model_id = self.invocation.model_id
                account_id = self.invocation.account_id

            event = InvocationEvent(
                invocation_id=invocation_id,
                model_id=model_id,
                account_id=account_id,
                stage=stage,
                status=status,
                flags=flags,
                is_flag_event=(flags is not None),
            )
            self._progress_tracker(event)

    async def create_invocation(self):
        self._update_progress("Invocation", "Starting")
        invocations = await Persist.invocations.create_invocations_by_model_names(self.__http_client, self.__token, {self.invocation_config["model_name"]: 1})
        self.invocation = invocations[0]
        self._update_progress("Invocation", "Waiting")

    async def upload_inputs(self):
        _tasks = []
        self._update_progress("Inputs_Upload", "Starting")

        for input in self.invocation.inputs:
            input_config = self.invocation_config["inputs"][input.display_title]
            input_source = input_config["source"]
            input_data = input_config["data"]
            if input_source == "Primitive":
                _tasks.append(self._upload_primitive(input, input_data))
            elif input_source == "Path":
                _tasks.append(self._upload_file(input, input_data))
            elif input_source == "Stream":
                _tasks.append(self._upload_buffer(input, input_data))
            else:
                _tasks.append(self._upload_link(input, input_data))

        await asyncio.gather(*_tasks)
        self._update_progress("Inputs_Upload", "Finished")

    async def wait_for_flags(self):
        self._update_progress("Flags", "Starting")
        while True:
            invocation_constraints = QueryConstraints.equals("Invocation_Flags", "Entity_ID", self.invocation.id)
            results = await Persist.invocation_flags.get_invocation_status_flags(self.__http_client, self.__token, invocation_constraints)
            if len(results) == 0:
                raise ValueError(f"Invocation {self.invocation.id} has no flags")

            all_flags: list[StatusFlag] = []
            for key in results:
                all_flags.extend(results[key])

            self._update_progress("Flags", "Waiting", all_flags)

            any_failed = any(flag.flag_value == StatusFlagValue.Error for flag in all_flags)
            all_finished = all(flag.flag_value == StatusFlagValue.Finished for flag in all_flags)

            if any_failed:
                raise ValueError(f"Invocation {self.invocation.id} has failed")
            if all_finished:
                break

            await asyncio.sleep(1)
        self._update_progress("Flags", "Finished")

    async def get_results(self) -> dict[str, bytearray]:
        self._update_progress("Results", "Starting")
        output_tasks = []
        for output in self.invocation.outputs:
            task = self.get_output_results(output.id)
            output_tasks.append(task)

        results: list[bytearray] = await asyncio.gather(*output_tasks)
        self._update_progress("Results", "Finished")
        self._update_progress("Invocation", "Finished")
        return {
            output.display_title: result
            for output, result in zip(self.invocation.outputs, results)
        }

    async def get_output_results(self, output_id: str) -> bytearray:
        http_client = self.__http_client
        token = self.__token
        account_id = self.invocation.account_id
        model_id = self.invocation.model_id
        invocation_id = self.invocation.id

        headers, stream = await Retrieve.get_invocation_output(http_client, token, account_id, model_id, invocation_id, output_id)

        results = bytearray()
        async for chunk in stream:
            results.extend(chunk)

        return results

    async def _upload_primitive(self, input: InvocationSignature, data: Any):
        buffer = io.BytesIO()
        buffer.write(str(data).encode("utf-8"))
        buffer.seek(0)

        await self._upload_buffer(input, buffer)

    async def _upload_file(self, input: InvocationSignature, file_path: str):
        async with aiofiles.open(file_path, mode="rb") as file:
            await self._upload_buffer(input, file)

    async def _upload_buffer(self, input: InvocationSignature, buffer: Union[AsyncBufferedReader, BufferedReader]):
        pairing_request = SocketInputPairingRequest(
            invocation_id=input.invocation_id,
            input_id=input.id,
            account_id=input.account_id,
            model_id=input.model_id,
            file_size_in_bytes=self._get_buffer_size(buffer),
        )
        request = await FilePush.allocate_socket_for_input(
            self.__http_client, self.__token, pairing_request
        )

        file_checksum = await SocketClient.write_and_digest(request, buffer)

        checksum_request = ChecksumRequest(
            pairing_id=request.pairing_id, checksum=file_checksum
        )
        await FilePush.complete_file_transfer(self.__http_client, self.__token, checksum_request)

    async def _upload_link(self, input: InvocationSignature, link: str):
        download_request = InputDownloadRequest(
            signature_id=input.signature_id,
            invocation_id=input.invocation_id,
            input_id=input.id,
            account_id=input.account_id,
            model_id=input.model_id,
            links=[link]
        )
        response = await FilePull.submit_input_links(self.__http_client, self.__token, download_request)
        return response

    @staticmethod
    def _get_buffer_size(file_obj):
        if hasattr(file_obj, "fileno"):
            return os.fstat(file_obj.fileno()).st_size
        if isinstance(file_obj, io.BytesIO):
            return file_obj.getbuffer().nbytes
        raise ValueError("Unsupported file object or operation")
