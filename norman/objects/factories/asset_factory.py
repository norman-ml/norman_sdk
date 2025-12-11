from norman_objects.shared.models.model_asset import ModelAsset
from norman_utils_external.singleton import Singleton

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig


class AssetFactory(metaclass=Singleton):
    authentication_manager = AuthenticationManager()

    @staticmethod
    def create(asset_config: AssetConfig) -> ModelAsset:
        asset = ModelAsset(
            account_id=AssetFactory.authentication_manager.account_id,
            asset_name=asset_config.asset_name
        )

        return asset
