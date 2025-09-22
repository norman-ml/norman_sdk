import asyncio
import io
import os
from typing import Any, Optional

import aiofiles
from norman_core.clients.http_client import HttpClient
from norman_core.clients.socket_client import SocketClient
from norman_core.services.file_pull.file_pull import FilePull
from norman_core.services.file_push.file_push import FilePush
from norman_core.services.persist import Persist
from norman_objects.services.file_pull.requests.asset_download_request import AssetDownloadRequest
from norman_objects.services.file_push.checksum.checksum_request import ChecksumRequest
from norman_objects.services.file_push.pairing.socket_asset_pairing_request import SocketAssetPairingRequest
from norman_objects.shared.models.model_asset import ModelAsset
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag import StatusFlag
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

from norman._helpers.model_from_config import ModelFromConfig
from norman.objects.trackers.model_upload_tracker import UploadEvent, UploadStage, UploadStatus, UploadTracker


class UploadManager:
    def __init__(self, http_client: HttpClient, token: Sensitive[str], account_id: str,  model_config: dict[str, Any], progress_tracker: Optional[UploadTracker] = None):
        self.__http_client = http_client
        self.__token = token
        self._account_id = account_id

        self.model = ModelFromConfig.create_model(account_id, model_config)
        self._assets = model_config["assets"]

        self._progress_tracker = progress_tracker

    async def upload_model(self):
        self._update_progress("Model_Upload", "Starting")
        response = await Persist.models.create_models(self.__http_client, self.__token, [self.model])

        if len(response) == 0:
            raise ValueError("Failed to create model")
        self.model = next(iter(response.values()))
        self._update_progress("Model_Upload", "Waiting")

    async def upload_assets(self):
        self._update_progress("Inputs_Upload", "Starting")
        tasks = []
        for model_asset in self.model.assets:
            asset = next(asset for asset in self._assets if asset["asset_name"] == model_asset.asset_name)
            asset_source = asset["source"]
            asset_data = asset["data"]

            if asset_source == "Link":
                tasks.append(self._upload_link(model_asset, asset_data))
            elif asset_source == "Path":
                tasks.append(self._upload_file(model_asset, asset_data))
            elif asset_source == "Stream":
                tasks.append(self._upload_buffer(model_asset, asset_data))
            else:
                raise ValueError("Model asset source must be one of link, path, or stream.")

        await asyncio.gather(*tasks)
        self._update_progress("Inputs_Upload", "Finished")

    async def _upload_link(self, model_asset: ModelAsset, link: str):
        download_request = AssetDownloadRequest(
            account_id=self._account_id,
            model_id=self.model.id,
            asset_id=model_asset.id,
            links=[link]
        )
        await FilePull.submit_asset_links(self.__http_client, self.__token, download_request)

    async def _upload_file(self, model_asset: ModelAsset, path: str):
        async with aiofiles.open(path, mode="rb") as file:
            await self._upload_buffer(model_asset, file)

    async def _upload_buffer(self, model_asset: ModelAsset, file_buffer: Any):
        pairing_request = SocketAssetPairingRequest(
            account_id=self._account_id,
            model_id=self.model.id,
            asset_id=model_asset.id,
            file_size_in_bytes=self._get_buffer_size(file_buffer),
        )
        socket_info = await FilePush.allocate_socket_for_asset(self.__http_client, self.__token, pairing_request)
        checksum = await SocketClient.write_and_digest(socket_info, file_buffer)

        checksum_request = ChecksumRequest(
            pairing_id=socket_info.pairing_id,
            checksum=checksum
        )
        await FilePush.complete_file_transfer(self.__http_client, self.__token, checksum_request)

    async def wait_for_flags(self):
        self._update_progress("Flags", "Starting")
        while True:
            model_flag_constraints = QueryConstraints.equals("Model_Flags", "Entity_ID", self.model.id)
            asset_flag_constraints = QueryConstraints.includes("Asset_Flags", "Entity_ID", [asset.id for asset in self.model.assets])

            model_flag_task = Persist.model_flags.get_model_status_flags(self.__http_client, self.__token, model_flag_constraints)
            asset_flag_task = Persist.model_flags.get_asset_status_flags(self.__http_client, self.__token, asset_flag_constraints)

            results = await asyncio.gather(model_flag_task, asset_flag_task)

            all_model_flags: list[StatusFlag] = [flag for flag_result in results for flag_list in flag_result.values() for flag in flag_list]
            self._update_progress("Flags", "Waiting", all_model_flags)

            failed_flags = [flag for flag in all_model_flags if flag.flag_value == StatusFlagValue.Error]
            if len(failed_flags) > 0:
                raise Exception("Failed to upload model", failed_flags)

            all_finished = all(flag.flag_value == StatusFlagValue.Finished for flag in all_model_flags)
            if all_finished:
                break
            await asyncio.sleep(5)
        self._update_progress("Flags", "Finished")
        self._update_progress("Model_Upload", "Finished")

    @staticmethod
    def _get_buffer_size(file_obj):
        if hasattr(file_obj, "fileno"):
            return os.fstat(file_obj.fileno()).st_size
        if isinstance(file_obj, io.BytesIO):
            return file_obj.getbuffer().nbytes
        raise ValueError("Unsupported file object or operation")

    def _update_progress(self, stage: UploadStage, status: UploadStatus = "Starting", flags: Optional[list[StatusFlag]] = None):
        if self._progress_tracker is not None:
            model_id = ""
            if self.model is not None:
                model_id = self.model.id
            event = UploadEvent(
                model_id=model_id,
                account_id=self._account_id,
                stage=stage,
                status=status,
                flags=flags,
                is_flag_event=(flags is not None),
            )
            self._progress_tracker(event)
