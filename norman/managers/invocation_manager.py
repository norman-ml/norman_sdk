import asyncio
import os
from typing import Any

from norman_core.clients.http_client import HttpClient
from norman_core.services.file_pull.file_pull import FilePull
from norman_core.services.persist import Persist
from norman_core.services.retrieve.retrieve import Retrieve
from norman_objects.services.file_pull.requests.input_download_request import InputDownloadRequest
from norman_objects.services.file_push.pairing.socket_input_pairing_request import SocketInputPairingRequest
from norman_objects.shared.invocation_signatures.invocation_signature import InvocationSignature
from norman_objects.shared.invocations.invocation import Invocation
from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.queries.filter_clauses.filter_clause import FilterClause
from norman_objects.shared.queries.filter_clauses.filter_node import FilterNode
from norman_objects.shared.queries.logical_relations.binary_relation import BinaryRelation
from norman_objects.shared.queries.logical_relations.unary_relation import UnaryRelation
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.security.sensitive import Sensitive
from norman_utils_external.file_utils import FileUtils

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.objects.configs.invocation.invocation_input_config import InvocationInputConfig
from norman.objects.handlers.response_handler import ResponseHandler
from norman.resolvers.flag_status_resolver import FlagStatusResolver
from norman.resolvers.input_source_resolver import InputSourceResolver
from norman.services.file_transfer_service import FileTransferService


class InvocationManager:
    def __init__(self) -> None:
        self._authentication_manager = AuthenticationManager()
        self._file_transfer_service = FileTransferService()
        self._file_utils = FileUtils()
        self._flag_status_resolver = FlagStatusResolver()
        self._http_client = HttpClient()

        self._file_pull_service = FilePull()
        self._persist_service = Persist()
        self._retrieve_service = Retrieve()

    async def invoke(self, invocation_config: dict[str, Any]) -> dict[str, bytes]:
        await self._authentication_manager.invalidate_access_token()
        validated_invocation_config = InvocationConfig.model_validate(invocation_config)

        async with self._http_client:
            model, invocation = await self.__create_invocation_and_select_model(self._authentication_manager.access_token, validated_invocation_config)
            await self.__upload_inputs(self._authentication_manager.access_token, model, invocation, validated_invocation_config)
            await self.__wait_for_flags(self._authentication_manager.access_token, invocation)
            output_handlers = await self.__get_response_handlers(self._authentication_manager.access_token, invocation)
            results = await self.__resolve_outputs(model, invocation, validated_invocation_config, output_handlers)

        return results

    async def __create_invocation_and_select_model(self, token: Sensitive[str], invocation_config: InvocationConfig) -> tuple[ModelProjection, Invocation]:
        model, invocation = await asyncio.gather(
            self.__select_model_from_database(token=token, invocation_config=invocation_config),
            self.__create_invocation_in_database(token=token, invocation_config=invocation_config)
        )

        return model, invocation

    async def __select_model_from_database(self, token: Sensitive[str], invocation_config: InvocationConfig) -> ModelProjection:
        model_select_constraint = QueryConstraints(
            filter=FilterClause(
                children=[
                    FilterNode(table="Models", column="Name", operator=BinaryRelation.EQ, value=invocation_config.model_name),
                    FilterNode(table="Models", column="Hidden", operator=BinaryRelation.EQ, value=False),
                    FilterNode(table="Model_Versions", column="Active", operator=BinaryRelation.EQ, value=True),
                ],
                join_condition=UnaryRelation.AND
            )
        )

        models = await self._persist_service.models.get_model_projections(token=token, constraint=model_select_constraint)
        if models is None or len(models) == 0:
            raise RuntimeError("Model selection failed")

        model = next(iter(models.values()))
        return model


    async def __create_invocation_in_database(self, token: Sensitive[str], invocation_config: InvocationConfig) -> Invocation:
        model_name_counter = {invocation_config.model_name: 1}

        invocations = await self._persist_service.invocations.create_invocations_by_model_names(token=token, model_name_counter=model_name_counter)
        if invocations is None or len(invocations) == 0:
            raise RuntimeError("Invocation creation failed")

        return invocations[0]

    async def __upload_inputs(self, token: Sensitive[str], model: ModelProjection, invocation: Invocation, invocation_config: InvocationConfig) -> None:
        model_input_signature_map = {input_signature.id: input_signature for input_signature in model.version.inputs}
        invocation_input_config_map = {input_config.display_title: input_config for input_config in invocation_config.inputs}

        for invocation_input in invocation.inputs:
            model_signature = model_input_signature_map[invocation_input.signature_id]
            input_config = invocation_input_config_map[model_signature.display_title]
            await self.__handle_input_upload(token, invocation_input, input_config)

    async def __handle_input_upload(self, token: Sensitive[str], invocation_input: InvocationSignature, input_config: InvocationInputConfig) -> None:
        data = input_config.data

        input_source = input_config.source
        if input_source is None:
            input_source = InputSourceResolver.resolve(data)

        if input_source == "Primitive":
            await self.__upload_primitive_input(token, invocation_input, data)
        elif input_source == "File":
            await self.__upload_file_input(token, invocation_input, data)
        elif input_source == "Stream":
            await self.__upload_stream_input(token, invocation_input, data)
        elif input_source == "Link":
            await self.__submit_link_input(token, invocation_input, data)
        else:
            raise ValueError(f"Unsupported input source: {input_source}")

    async def __upload_primitive_input(self, token: Sensitive[str], invocation_input: InvocationSignature, data: Any) -> None:
        byte_buffer = self._file_transfer_service.normalize_primitive_data(data)
        buffer_size = self._file_utils.get_buffer_size(byte_buffer)
        pairing_request = SocketInputPairingRequest(
            invocation_id=invocation_input.invocation_id,
            input_id=invocation_input.id,
            account_id=invocation_input.account_id,
            model_id=invocation_input.model_id,
            version_id=invocation_input.version_id,
            file_size_in_bytes=buffer_size
        )

        await self._file_transfer_service.upload_from_buffer(token, pairing_request, byte_buffer)

    async def __upload_file_input(self, token: Sensitive[str], invocation_input: InvocationSignature, path: str) -> None:
        file_size = os.path.getsize(path)
        pairing_request = SocketInputPairingRequest(
            invocation_id=invocation_input.invocation_id,
            input_id=invocation_input.id,
            account_id=invocation_input.account_id,
            model_id=invocation_input.model_id,
            version_id=invocation_input.version_id,
            file_size_in_bytes=file_size
        )
        await self._file_transfer_service.upload_file(token, pairing_request, path)

    async def __upload_stream_input(self, token: Sensitive[str], invocation_input: InvocationSignature, stream: Any) -> None:
        file_size = self._file_utils.get_buffer_size(stream)
        pairing_request = SocketInputPairingRequest(
            invocation_id=invocation_input.invocation_id,
            input_id=invocation_input.id,
            account_id=invocation_input.account_id,
            model_id=invocation_input.model_id,
            version_id=invocation_input.version_id,
            file_size_in_bytes=file_size
        )
        await self._file_transfer_service.upload_from_buffer(token, pairing_request, stream)

    async def __submit_link_input(self, token: Sensitive[str], invocation_input: InvocationSignature, link: str) -> None:
        download_request = InputDownloadRequest(
            signature_id=invocation_input.signature_id,
            invocation_id=invocation_input.invocation_id,
            input_id=invocation_input.id,
            account_id=invocation_input.account_id,
            model_id=invocation_input.model_id,
            version_id=invocation_input.version_id,
            links=[link],
        )
        await self._file_pull_service.submit_input_links(token, download_request)

    async def __wait_for_flags(self, token: Sensitive[str], invocation: Invocation) -> None:
        entity_ids = [invocation.id]
        entity_ids.extend([input.id for input in invocation.inputs])
        entity_ids.extend([output.id for output in invocation.outputs])

        await self._flag_status_resolver.wait_for_entities(token, entity_ids)

    async def __get_response_handlers(self, token: Sensitive[str], invocation: Invocation) -> dict[str, ResponseHandler]:
        response_handlers = {}

        for output in invocation.outputs:
            invocation_output = self._retrieve_service.get_invocation_output(token, invocation.account_id, invocation.version_id, invocation.id, output.id)
            response_handlers[output.id] = ResponseHandler(invocation_output)

        return response_handlers

    async def __resolve_outputs(self, model: ModelProjection, invocation: Invocation, invocation_config: InvocationConfig, response_handlers: dict[str, ResponseHandler]) -> dict[str, bytes]:
        model_output_signature_map = {output_signature.id: output_signature for output_signature in model.version.outputs}
        invocation_output_config_map = {output_config.display_title: output_config for output_config in invocation_config.outputs}

        invocation_results = {}
        for invocation_output in invocation.outputs:
            response_handler = response_handlers[invocation_output.id]
            model_signature = model_output_signature_map[invocation_output.signature_id]
            output_config = invocation_output_config_map[model_signature.display_title]

            display_title = output_config.display_title
            consume_mode = output_config.consume_mode

            if consume_mode is None:
                method = response_handler.bytes
            else:
                if not hasattr(response_handler, consume_mode.value):
                    raise ValueError(f"Unsupported response handler method: {consume_mode}")

                method = getattr(response_handler, consume_mode.value)

            invocation_results[display_title] = await method()

        return invocation_results
