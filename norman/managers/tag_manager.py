from norman_core.clients.http_client import HttpClient
from norman_core.services.persist import Persist
from norman_objects.shared.models.model_tag import ModelTag
from norman_objects.shared.queries.query_constraints import QueryConstraints

from norman.managers.authentication_manager import AuthenticationManager


class TagManager:
    def __init__(self) -> None:
        self._authentication_manager = AuthenticationManager()
        self._http_client = HttpClient()
        self._persist_service = Persist()

    async def tag_model(self, model_name: str, tag_name: str) -> ModelTag:
        await self._authentication_manager.invalidate_access_token()
        async with self._http_client:
            constraints = QueryConstraints.equals("Models", "Name", model_name)
            models = await self._persist_service.models.get_models(self._authentication_manager.access_token, constraints)
            model = next(iter(models.values()))
            model_id = model.id
            model_tag = ModelTag(account_id=self._authentication_manager.account_id, model_id=model_id, name=tag_name)
            new_tag = await self._persist_service.tags.create_tags(self._authentication_manager.access_token, [model_tag])

        return new_tag

    async def untag_model(self, model_name: str, tag_name: str) -> int:
        await self._authentication_manager.invalidate_access_token()
        async with self._http_client:
            model_constraints = QueryConstraints.equals("Models", "Name", model_name)
            models = await self._persist_service.models.get_models(self._authentication_manager.access_token, model_constraints)
            model = next(iter(models.values()))
            model_id = model.id
            tag_model_id_constraints = QueryConstraints.equals("Model_Tags", "Model_ID", model_id)
            tag_name_constraints = QueryConstraints.equals("Model_Tags", "Name", tag_name)
            tag_constraints = tag_model_id_constraints & tag_name_constraints
            return await self._persist_service.tags.delete_tags(self._authentication_manager.access_token, tag_constraints)
