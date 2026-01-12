import pytest
from norman_core.clients.http_client import HttpClient
from norman_objects.shared.models.model import Model
from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.status_flags.status_flag import StatusFlag
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue
from pydantic import TypeAdapter

from norman.managers.authentication_manager import AuthenticationManager
from tests.conftest import UploadedModelResult


@pytest.mark.integration
class TestModelUpload:
    @pytest.mark.models
    async def test_create_model(self, uploaded_model: UploadedModelResult) -> None:
        model_projection: ModelProjection = uploaded_model.model

        assert model_projection is not None
        assert isinstance(model_projection, ModelProjection)
        assert model_projection.id != "0"
        assert model_projection.account_id
        assert model_projection.name
        assert model_projection.category
        assert model_projection.version is not None
        assert model_projection.version.id != "0"

    @pytest.mark.models
    async def test_upload_model_configuration(self, api_key: str, uploaded_model: UploadedModelResult) -> None:
        model_projection: ModelProjection = uploaded_model.model
        model_config: dict = uploaded_model.config

        authentication_manager = AuthenticationManager()
        authentication_manager.set_api_key(api_key)
        await authentication_manager.invalidate_access_token()

        async with HttpClient() as http_client:
            query_constraints = QueryConstraints.equals("Models", "ID", model_projection.id)
            response = await http_client.post(
                "/persist/models/get",
                authentication_manager.access_token,
                json=query_constraints.model_dump(mode="json"),
            )

        assert response is not None
        assert isinstance(response, dict)
        assert model_projection.id in response

        models_by_id = TypeAdapter(dict[str, Model]).validate_python(response)
        db_model: Model = models_by_id[model_projection.id]

        assert db_model.name == model_config["name"]
        assert db_model.category == model_config["category"]
        assert db_model.account_id == model_projection.account_id
        assert len(db_model.versions) > 0

        db_version = next((v for v in db_model.versions if v.id == model_projection.version.id), None)
        assert db_version is not None

        version_config: dict = model_config["version"]
        assert db_version.label == version_config["label"]
        assert db_version.short_description == version_config["short_description"]
        assert db_version.long_description == version_config["long_description"]
        assert len(db_version.assets) == len(version_config["assets"])
        assert len(db_version.inputs) == len(version_config["inputs"])
        assert len(db_version.outputs) == len(version_config["outputs"])

        expected_asset_names: set[str] = {asset["asset_name"] for asset in version_config["assets"]}
        actual_asset_names: set[str] = {asset.asset_name for asset in db_version.assets}
        assert actual_asset_names == expected_asset_names

        expected_tag_names: set[str] = {tag["name"] for tag in model_config["user_tags"]}
        actual_tag_names: set[str] = {tag.name for tag in db_model.user_tags}
        assert actual_tag_names == expected_tag_names

    @pytest.mark.models
    async def test_upload_model_asset(self, uploaded_model: UploadedModelResult) -> None:
        model_projection: ModelProjection = uploaded_model.model
        model_config: dict = uploaded_model.config

        assert model_projection.version.assets is not None
        assert len(model_projection.version.assets) > 0

        expected_assets: dict = {asset["asset_name"]: asset for asset in model_config["version"]["assets"]}

        for model_asset in model_projection.version.assets:
            assert model_asset.id != "0"
            assert model_asset.model_id == model_projection.id
            assert model_asset.version_id == model_projection.version.id
            assert model_asset.account_id == model_projection.account_id
            assert model_asset.asset_name in expected_assets

        actual_asset_names: set[str] = {asset.asset_name for asset in model_projection.version.assets}
        expected_asset_names: set[str] = set(expected_assets.keys())
        assert actual_asset_names == expected_asset_names

    @pytest.mark.models
    async def test_wait_for_status_flags(self, api_key: str, uploaded_model: UploadedModelResult) -> None:
        model_projection: ModelProjection = uploaded_model.model
        entity_ids: list[str] = uploaded_model.entity_ids

        authentication_manager = AuthenticationManager()
        authentication_manager.set_api_key(api_key)
        await authentication_manager.invalidate_access_token()

        async with HttpClient() as http_client:
            query_constraints = QueryConstraints.includes("Status_Flags", "Entity_ID", entity_ids)
            response = await http_client.post(
                "/persist/flags/get",
                authentication_manager.access_token,
                json=query_constraints.model_dump(mode="json"),
            )

        assert response is not None

        status_flags_by_entity = TypeAdapter(dict[str, list[StatusFlag]]).validate_python(response)
        assert isinstance(status_flags_by_entity, dict)

        all_flags: list[StatusFlag] = []
        for entity_id, flags in status_flags_by_entity.items():
            assert isinstance(entity_id, str)
            assert isinstance(flags, list)
            all_flags.extend(flags)

        assert len(all_flags) > 0

        for status_flag in all_flags:
            assert isinstance(status_flag, StatusFlag)
            assert status_flag.id != "0"
            assert status_flag.entity_id in entity_ids
            assert status_flag.account_id == model_projection.account_id
            assert isinstance(status_flag.flag_value, StatusFlagValue)
            assert status_flag.flag_value == StatusFlagValue.Finished
