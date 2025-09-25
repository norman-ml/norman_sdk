import asyncio
import io
import os
from typing import Any

import aiofiles
from norman_core.clients.http_client import HttpClient
from norman_core.clients.socket_client import SocketClient
from norman_core.services.file_pull.file_pull import FilePull
from norman_core.services.file_push.file_push import FilePush
from norman_core.services.persist import Persist
from norman_objects.services.file_pull.requests.asset_download_request import AssetDownloadRequest
from norman_objects.services.file_push.checksum.checksum_request import ChecksumRequest
from norman_objects.services.file_push.pairing.socket_asset_pairing_request import SocketAssetPairingRequest
from norman_objects.shared.models.model import Model
from norman_objects.shared.models.model_asset import ModelAsset
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag import StatusFlag
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

from norman.helpers.get_buffer_size import get_buffer_size


class ModelUploadManager:
    @staticmethod
    async def upload_model(http_client: HttpClient, token: Sensitive[str], model: Model) -> Model:
        response = await Persist.models.create_models(http_client, token, [model])

        if len(response) == 0:
            raise ValueError("Failed to create model")
        return next(iter(response.values()))

    @staticmethod
    async def upload_assets(http_client: HttpClient, token: Sensitive[str], model: Model, assets: list[dict[str, Any]]):
        tasks = []
        for model_asset in model.assets:
            asset = next(asset for asset in assets if asset["asset_name"] == model_asset.asset_name)
            asset_source = asset["source"]
            asset_data = asset["data"]

            if asset_source == "Link":
                tasks.append(ModelUploadManager._upload_link(http_client, token, model, model_asset, asset_data))
            elif asset_source == "Path":
                tasks.append(ModelUploadManager._upload_file(http_client, token, model, model_asset, asset_data))
            elif asset_source == "Stream":
                tasks.append(ModelUploadManager._upload_buffer(http_client, token, model, model_asset, asset_data))
            else:
                raise ValueError("Model asset source must be one of link, path, or stream.")

        await asyncio.gather(*tasks)

    @staticmethod
    async def _upload_link(http_client: HttpClient, token: Sensitive[str], model: Model, model_asset: ModelAsset, link: str):
        download_request = AssetDownloadRequest(
            account_id=model.account_id,
            model_id=model.id,
            asset_id=model_asset.id,
            links=[link]
        )
        await FilePull.submit_asset_links(http_client, token, download_request)

    @staticmethod
    async def _upload_file(http_client: HttpClient, token: Sensitive[str], model: Model, model_asset: ModelAsset, path: str):
        async with aiofiles.open(path, mode="rb") as file:
            await ModelUploadManager._upload_buffer(http_client, token, model, model_asset, file)

    @staticmethod
    async def _upload_buffer(http_client: HttpClient, token: Sensitive[str], model: Model, model_asset: ModelAsset, file_buffer: Any):
        pairing_request = SocketAssetPairingRequest(
            account_id=model.account_id,
            model_id=model.id,
            asset_id=model_asset.id,
            file_size_in_bytes=get_buffer_size(file_buffer),
        )
        socket_info = await FilePush.allocate_socket_for_asset(http_client, token, pairing_request)
        checksum = await SocketClient.write_and_digest(socket_info, file_buffer)

        checksum_request = ChecksumRequest(
            pairing_id=socket_info.pairing_id,
            checksum=checksum
        )
        await FilePush.complete_file_transfer(http_client, token, checksum_request)

    @staticmethod
    async def wait_for_flags(http_client: HttpClient, token: Sensitive[str], model: Model):
        while True:
            model_flag_constraints = QueryConstraints.equals("Model_Flags", "Entity_ID", model.id)
            asset_flag_constraints = QueryConstraints.includes("Asset_Flags", "Entity_ID", [asset.id for asset in model.assets])

            model_flag_task = Persist.model_flags.get_model_status_flags(http_client, token, model_flag_constraints)
            asset_flag_task = Persist.model_flags.get_asset_status_flags(http_client, token, asset_flag_constraints)

            results = await asyncio.gather(model_flag_task, asset_flag_task)

            all_model_flags: list[StatusFlag] = [flag for flag_result in results for flag_list in flag_result.values() for flag in flag_list]

            failed_flags = [flag for flag in all_model_flags if flag.flag_value == StatusFlagValue.Error]
            if len(failed_flags) > 0:
                raise Exception("Failed to upload model", failed_flags)

            all_finished = all(flag.flag_value == StatusFlagValue.Finished for flag in all_model_flags)
            if all_finished:
                break
            await asyncio.sleep(5)

    @staticmethod
    def _get_buffer_size(file_obj):
        if hasattr(file_obj, "fileno"):
            return os.fstat(file_obj.fileno()).st_size
        if isinstance(file_obj, io.BytesIO):
            return file_obj.getbuffer().nbytes
        raise ValueError("Unsupported file object or operation")
