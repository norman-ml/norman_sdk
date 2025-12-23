from unittest.mock import patch, PropertyMock

from norman_objects.shared.models.model_asset import ModelAsset

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.factories.asset_factory import AssetFactory
from norman.managers.authentication_manager import AuthenticationManager


class TestAssetFactoryCreate:
    def test_create_returns_model_asset_instance(self):
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert isinstance(result, ModelAsset)

    def test_create_sets_asset_name_from_config(self):
        config = AssetConfig(asset_name="my-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.asset_name == "my-asset"

    def test_create_sets_account_id_from_authentication_manager(self):
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-456"
            result = AssetFactory.create(config)

        assert result.account_id == "account-456"

    def test_create_sets_default_id(self):
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.id == "0"

    def test_create_sets_default_model_id(self):
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.model_id == "0"

    def test_create_sets_default_version_id(self):
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.version_id == "0"

    def test_create_preserves_asset_name_with_special_characters(self):
        config = AssetConfig(asset_name="my-asset_v1.0", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.asset_name == "my-asset_v1.0"

    def test_create_preserves_asset_name_with_spaces(self):
        config = AssetConfig(asset_name="asset with spaces", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.asset_name == "asset with spaces"

    def test_create_with_string_data(self):
        config = AssetConfig(asset_name="test-asset", data="string data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.asset_name == "test-asset"

    def test_create_with_path_data(self):
        from pathlib import Path
        config = AssetConfig(asset_name="test-asset", data=Path("/some/path/to/file"))

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.asset_name == "test-asset"

    def test_create_multiple_assets_with_different_names(self):
        config1 = AssetConfig(asset_name="asset-one", data=b"data 1")
        config2 = AssetConfig(asset_name="asset-two", data=b"data 2")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result1 = AssetFactory.create(config1)
            result2 = AssetFactory.create(config2)

        assert result1.asset_name == "asset-one"
        assert result2.asset_name == "asset-two"
        assert result1.account_id == result2.account_id
