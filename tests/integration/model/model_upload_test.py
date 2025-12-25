import pytest
from norman_core.clients.http_client import HttpClient
from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.queries.query_constraints import QueryConstraints
from norman_objects.shared.status_flags.status_flag import StatusFlag
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue
from pydantic import TypeAdapter

from norman.managers.authentication_manager import AuthenticationManager
from tests.conftest import UploadedModelResult


@pytest.mark.usefixtures("api_key")
class TestModelUpload:
    @pytest.mark.models
    async def test_create_model(self, uploaded_model: UploadedModelResult) -> None:
        model = uploaded_model.model

        assert model is not None
        assert isinstance(model, ModelProjection)

        assert model.id != "0", "Model ID should be assigned by the server"
        assert model.account_id, "Account ID should be populated"
        assert model.name, "Model name should be populated"
        assert model.category, "Model category should be populated"

        assert model.version is not None
        assert model.version.id != "0", "Version ID should be assigned by the server"

        print(f"\nCreated model: {model.model_dump()}")

    @pytest.mark.models
    async def test_upload_model_configuration(self, api_key: str, uploaded_model: UploadedModelResult) -> None:
        model = uploaded_model.model
        config = uploaded_model.config

        auth_manager = AuthenticationManager()
        auth_manager.set_api_key(api_key)
        await auth_manager.invalidate_access_token()

        async with HttpClient() as client:
            constraints = QueryConstraints.equals("Models", "ID", model.id)
            response = await client.post(
                "/persist/models/get",
                auth_manager.access_token,
                json=constraints.model_dump(mode="json")
            )

        assert response is not None, "Should receive a response from the server"
        assert isinstance(response, dict), "Response should be a dictionary"
        assert model.id in response, f"Model {model.id} should exist in database"

        from norman_objects.shared.models.model import Model
        models_by_id = TypeAdapter(dict[str, Model]).validate_python(response)

        db_model = models_by_id[model.id]

        assert db_model.name == config["name"], "Model name should match configuration"
        assert db_model.category == config["category"], "Model category should match configuration"
        assert db_model.account_id == model.account_id, "Account ID should match"

        assert len(db_model.versions) > 0, "Model should have at least one version"

        db_version = next((v for v in db_model.versions if v.id == model.version.id), None)
        assert db_version is not None, f"Version {model.version.id} should exist in database"

        version_config = config["version"]
        assert db_version.label == version_config["label"], "Version label should match"
        assert db_version.short_description == version_config["short_description"], "Short description should match"
        assert db_version.long_description == version_config["long_description"], "Long description should match"

        expected_asset_count = len(version_config["assets"])
        actual_asset_count = len(db_version.assets)
        assert actual_asset_count == expected_asset_count, f"Expected {expected_asset_count} assets, got {actual_asset_count}"

        expected_asset_names = {asset["asset_name"] for asset in version_config["assets"]}
        actual_asset_names = {asset.asset_name for asset in db_version.assets}
        assert actual_asset_names == expected_asset_names, "Asset names should match configuration"

        expected_input_count = len(version_config["inputs"])
        actual_input_count = len(db_version.inputs)
        assert actual_input_count == expected_input_count, f"Expected {expected_input_count} inputs, got {actual_input_count}"

        expected_output_count = len(version_config["outputs"])
        actual_output_count = len(db_version.outputs)
        assert actual_output_count == expected_output_count, f"Expected {expected_output_count} outputs, got {actual_output_count}"

        expected_tag_names = {tag["name"] for tag in config["user_tags"]}
        actual_tag_names = {tag.name for tag in db_model.user_tags}
        assert actual_tag_names == expected_tag_names, "User tags should match configuration"

        print(f"\nVerified model configuration in database: {db_model.id}")
        print(f"  - Name: {db_model.name}")
        print(f"  - Category: {db_model.category}")
        print(f"  - Version: {db_version.label}")
        print(f"  - Assets: {actual_asset_names}")
        print(f"  - Tags: {actual_tag_names}")

    @pytest.mark.models
    async def test_upload_model_asset(self, api_key: str, uploaded_model: UploadedModelResult) -> None:
        model = uploaded_model.model
        config = uploaded_model.config

        assert model.version.assets is not None, "Model should have assets"
        assert len(model.version.assets) > 0, "Model should have at least one asset"

        expected_assets = {asset["asset_name"]: asset for asset in config["version"]["assets"]}

        for asset in model.version.assets:
            assert asset.id != "0", f"Asset '{asset.asset_name}' should have a valid ID"
            assert asset.model_id == model.id, f"Asset '{asset.asset_name}' should be associated with the model"
            assert asset.version_id == model.version.id, f"Asset '{asset.asset_name}' should be associated with the version"
            assert asset.account_id == model.account_id, f"Asset '{asset.asset_name}' should have correct account ID"
            assert asset.asset_name in expected_assets, f"Asset '{asset.asset_name}' should be in configuration"

            print(f"\nVerified asset: {asset.asset_name} (ID: {asset.id})")

        actual_asset_names = {asset.asset_name for asset in model.version.assets}
        expected_asset_names = set(expected_assets.keys())
        assert actual_asset_names == expected_asset_names, "All configured assets should be created"

    @pytest.mark.models
    async def test_wait_for_status_flags(self, api_key: str, uploaded_model: UploadedModelResult) -> None:
        model = uploaded_model.model
        entity_ids = uploaded_model.entity_ids

        auth_manager = AuthenticationManager()
        auth_manager.set_api_key(api_key)
        await auth_manager.invalidate_access_token()

        async with HttpClient() as client:
            constraints = QueryConstraints.includes("Status_Flags", "Entity_ID", entity_ids)

            response = await client.post(
                "/persist/flags/get",
                auth_manager.access_token,
                json=constraints.model_dump(mode="json")
            )

            # checks that response is received
            assert response is not None, "Should receive a response from the status flags endpoint"

            # checks that deserializes correctly
            status_flags_by_entity = TypeAdapter(dict[str, list[StatusFlag]]).validate_python(response)
            assert isinstance(status_flags_by_entity, dict), "Response should deserialize to a dictionary"

            # valid structure
            all_flags: list[StatusFlag] = []
            for entity_id, flags in status_flags_by_entity.items():
                assert isinstance(entity_id, str), "Entity ID should be a string"
                assert isinstance(flags, list), "Flags should be a list"
                all_flags.extend(flags)

            assert len(all_flags) > 0, "Should have at least one status flag"

            # valid flag values
            for flag in all_flags:
                assert isinstance(flag, StatusFlag), "Each flag should be a StatusFlag instance"
                assert flag.id != "0", "Flag should have a valid ID"
                assert flag.entity_id in entity_ids, f"Flag entity_id '{flag.entity_id}' should be one of our entity IDs"
                assert flag.account_id == model.account_id, "Flag should have correct account ID"
                assert isinstance(flag.flag_value, StatusFlagValue), "Flag value should be a StatusFlagValue enum"

                assert flag.flag_value == StatusFlagValue.Finished, \
                    f"Flag for entity {flag.entity_id} should be Finished after successful upload, got {flag.flag_value}"

                print(f"\nVerified flag: entity={flag.entity_id}, name={flag.flag_name}, value={flag.flag_value}")

        print(f"\nSuccessfully verified {len(all_flags)} status flags for {len(entity_ids)} entities")
