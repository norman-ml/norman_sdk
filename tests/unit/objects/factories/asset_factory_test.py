import pytest
from unittest.mock import patch, PropertyMock

from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.models.model_asset import ModelAsset

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.factories.asset_factory import AssetFactory
from norman.managers.authentication_manager import AuthenticationManager


@pytest.fixture
def mock_account_id():
    with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock:
        mock.return_value = "account-123"
        yield mock


@pytest.mark.usefixtures("mock_account_id")
class TestAssetFactory:

    def test_create_returns_model_asset_instance(self) -> None:
        config = AssetConfig(asset_name="model-weights.pt", data=b"binary weights", source=InputSource.Primitive)

        created_asset = AssetFactory.create(config)

        assert isinstance(created_asset, ModelAsset)
        assert created_asset.asset_name == "model-weights.pt"
        assert created_asset.id == "0"
        assert created_asset.model_id == "0"
        assert created_asset.version_id == "0"

    def test_create_sets_account_id_from_authentication_manager(self) -> None:
        config = AssetConfig(asset_name="weights.pt", data=b"data", source=InputSource.Primitive)

        created_asset = AssetFactory.create(config)

        assert created_asset.account_id == "account-123"
