from norman_objects.shared.modality.container_modality import ContainerModality
from norman_objects.shared.models.model_asset import ModelAsset
from norman_utils_external.singleton import Singleton

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.asset_config import AssetConfig


class AssetFactory(metaclass=Singleton):
    authentication_manager = AuthenticationManager()

    @staticmethod
    def create(asset_config: AssetConfig) -> ModelAsset:
        if asset_config.asset_name == "Logo":
            data_modality = ContainerModality.Image
        elif asset_config.asset_name == "File":
            data_modality = ContainerModality.File
        else:
            raise ValueError("Asset name not supported")

        asset = ModelAsset(
            id=asset_config.id,
            account_id=AssetFactory.authentication_manager.account_id,
            model_id=asset_config.model_id,
            version_id=asset_config.version_id,
            asset_name=asset_config.asset_name,
            data_modality=data_modality
        )

        return asset
