import pytest

from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.models.model_asset import ModelAsset

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.factories.asset_factory import AssetFactory
from tests.constants import DEFAULT_ID, DEFAULT_MODEL_ID, DEFAULT_VERSION_ID, TEST_ACCOUNT_ID


@pytest.mark.unit
@pytest.mark.factory
@pytest.mark.usefixtures("account_id_mock")
class TestAssetFactory:
    def test_create_returns_model_asset_instance(self) -> None:
        asset_name = "model-weights.pt"
        data = b"binary weights"

        asset_config = AssetConfig(
            asset_name=asset_name,
            data=data,
            source=InputSource.Primitive,
        )

        model_asset = AssetFactory.create(asset_config)

        assert isinstance(model_asset, ModelAsset)
        assert model_asset.asset_name == asset_name
        assert model_asset.id == DEFAULT_ID
        assert model_asset.model_id == DEFAULT_MODEL_ID
        assert model_asset.version_id == DEFAULT_VERSION_ID

    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        asset_config = AssetConfig(
            asset_name="weights.pt",
            data=b"data",
            source=InputSource.Primitive,
        )

        model_asset = AssetFactory.create(asset_config)

        assert model_asset.account_id == TEST_ACCOUNT_ID
