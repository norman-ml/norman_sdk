import asyncio
import io
import os
from dataclasses import dataclass
from typing import Any, Callable, Optional, Literal

import aiofiles
from norman_core.clients.http_client import HttpClient
from norman_core.clients.socket_client import SocketClient
from norman_core.services.file_pull.file_pull import FilePull
from norman_core.services.file_push.file_push import FilePush
from norman_core.services.persist import Persist
from norman_objects.services.file_pull.requests.asset_download_request import AssetDownloadRequest
from norman_objects.services.file_push.checksum.checksum_request import ChecksumRequest
from norman_objects.services.file_push.pairing.socket_asset_pairing_request import SocketAssetPairingRequest
from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model import Model
from norman_objects.shared.models.model_asset import ModelAsset
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.output_format import OutputFormat
from norman_objects.shared.parameters.data_domain import DataDomain
from norman_objects.shared.parameters.model_param import ModelParam
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag import StatusFlag
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

from norman.objects.trackers.model_upload_tracker import UploadEvent, _UploadStage, _UploadStatus, UploadTracker


class UploadManager:
    def __init__(self, http_client: HttpClient, token: Sensitive[str], account_id: str,  model_config: dict[str, Any], progress_tracker: Optional[UploadTracker] = None):
        self.__http_client = http_client
        self.__token = token
        self._account_id = account_id

        self.assets = {}
        self._progress_tracker = progress_tracker

        self.model = self._create_model(model_config)

    async def upload_model(self):
        self._update_progress("Model_Upload", "Starting")
        response = await Persist.models.create_models(self.__http_client, self.__token, [self.model])

        if len(response) == 0:
            raise ValueError("Failed to create model")
        self.model = next(iter(response.values()))
        self._update_progress("Model_Upload", "Waiting")

    def _create_model(self, minimal_config: dict[str, Any]):
        config: dict[str, Any] = {
            "version_label": "v1.0",
            "hosting_location": "Internal",
            "output_format": "Json",
            "request_type": "Post",
            "http_headers": {},
            "url": ""
        }

        config.update(minimal_config)
        config["model_type"] = "Pytorch_jit" if config["hosting_location"] == "Internal" else "Api"

        self._validate_fields(config)

        model = Model(
            name = config["name"],
            account_id = self._account_id,
            version_label = config["version_label"],
            short_description = config["short_description"],
            url = config["url"],
            request_type = HttpRequestType(config["request_type"]),
            model_type = ModelType(config["model_type"]),
            hosting_location = ModelHostingLocation(config["hosting_location"]),
            output_format = OutputFormat(config["output_format"]),
            long_description = config["long_description"],
            http_headers = config["http_headers"],
        )

        model.inputs = self._create_model_signatures(SignatureType.Input, config["inputs"])
        model.outputs = self._create_model_signatures(SignatureType.Output, config["outputs"])
        model.assets = self._create_model_assets(config["assets"])

        return model

    def _create_model_signatures(self, signature_type: SignatureType, signature_configs: list[dict[str, Any]]):
        basic_config: dict[str, Any] = {
            "receive_format": "File",
            "http_location": "Body",
            "default_value": None
        }
        signatures = []
        for signature_config in signature_configs:
            config = basic_config.copy()
            config.update(signature_config)

            self._validate_signature_fields(config)

            model_signature = ModelSignature(
                display_title = config["display_title"],
                signature_type = signature_type,
                data_domain = DataDomain(config["data_domain"]),
                data_encoding = config["data_encoding"],
                receive_format = ReceiveFormat(config["receive_format"]),
                http_location = HttpLocation(config["http_location"]),
                hidden = False,
                default_value = config["default_value"],
            )

            for param in config["parameters"]:
                model_param = ModelParam(
                    parameter_name=param["parameter_name"],
                    data_domain=param["data_domain"],
                    data_encoding=param["data_encoding"]
                )
                model_signature.parameters.append(model_param)


            signatures.append(model_signature)

        return signatures

    def _create_model_assets(self, assets: list[dict[str, Any]]):
        model_assets: list[ModelAsset] = []
        for asset in assets:
            model_asset = ModelAsset(
                account_id = self._account_id,
                asset_name=asset["asset_name"],
            )
            model_assets.append(model_asset)
            self.assets[model_asset.asset_name] = asset

        return model_assets

    @staticmethod
    def _validate_signature_fields(config: dict[str, Any]):
        required_fields = ["display_title", "data_domain", "data_encoding", "parameters"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"{field} must be provided")

        if len(config["parameters"]) == 0:
            raise ValueError("parameters must be provided")

        required_param_fields = ["parameter_name", "data_domain", "data_encoding"]
        for param in config["parameters"]:
            for field in required_param_fields:
                if field not in param:
                    raise ValueError(f"{field} must be provided for each parameter")

    @staticmethod
    def _validate_fields(config: dict[str, Any]):
        required_fields = ["name", "short_description", "long_description", "inputs", "outputs", "assets"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"{field} must be provided")

        if config["hosting_location"] == "External":
            if "url" not in config or config["url"] == "":
                raise ValueError("url must be provided when hosting_location is external")
            if "http_headers" not in config:
                raise ValueError("http_headers must be provided when request_type is Post")

    async def upload_assets(self):
        self._update_progress("Inputs_Upload", "Starting")
        tasks = []
        for model_asset in self.model.assets:
            asset = self.assets[model_asset.asset_name]
            asset_source = asset["source"]
            asset_data = asset["data"]

            if asset_source == "link":
                tasks.append(self._upload_link(model_asset, asset_data))
            elif asset_source == "path":
                tasks.append(self._upload_file(model_asset, asset_data))
            elif asset_source == "stream":
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

    def _update_progress(self, stage: _UploadStage, status: _UploadStatus = "Starting", flags: Optional[list[StatusFlag]] = None):
        if self._progress_tracker is not None:
            event = UploadEvent(
                model_id=self.model.id if self.model else "",
                account_id=self._account_id,
                stage=stage,
                status=status,
                flags=flags,
                is_flag_event=(flags is not None),
            )
            self._progress_tracker(event)
