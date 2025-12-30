from norman_objects.shared.files.partition_name import PartitionName
from norman_objects.shared.model_assets.asset_name import AssetName
from norman_objects.shared.model_assets.model_asset import ModelAsset
from norman_utils_external.singleton import Singleton

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig


class AssetFactory(metaclass=Singleton):
    authentication_manager = AuthenticationManager()

    @staticmethod
    def create(asset_config: AssetConfig) -> ModelAsset:
        if asset_config.asset_name == AssetName.Logo:
            partition_name = PartitionName.Ephemeral
        else:
            partition_name = PartitionName.Permanent

        asset = ModelAsset(
            account_id=AssetFactory.authentication_manager.account_id,
            asset_name=asset_config.asset_name,
            partition_name=partition_name
        )

        return asset
