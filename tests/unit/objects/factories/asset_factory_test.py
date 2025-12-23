import pytest
from unittest.mock import patch, PropertyMock

from pydantic import ValidationError

from norman_objects.shared.models.model_asset import ModelAsset

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.factories.asset_factory import AssetFactory
from norman.managers.authentication_manager import AuthenticationManager


class TestAssetFactoryCreate:
    """Tests for AssetFactory.create() method"""

    # --- Success Cases ---

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

    # --- Default Values ---

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

    # --- Asset Name Variations ---

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

    def test_create_preserves_empty_asset_name(self):
        config = AssetConfig(asset_name="", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.asset_name == ""

    # --- Different Data Types in Config ---

    def test_create_with_bytes_data(self):
        config = AssetConfig(asset_name="test-asset", data=b"binary data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "account-123"
            result = AssetFactory.create(config)

        assert result.asset_name == "test-asset"

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

    # --- Account ID Edge Cases ---

    def test_create_with_none_account_id_raises_validation_error(self):
        """ModelAsset requires account_id to be a string, not None"""
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = None

            with pytest.raises(ValidationError):
                AssetFactory.create(config)

    def test_create_with_empty_account_id(self):
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = ""
            result = AssetFactory.create(config)

        assert result.account_id == ""

    # --- Multiple Calls ---

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


class TestAssetFactoryAuthenticationManagerIntegration:
    """Tests verifying AssetFactory's integration with AuthenticationManager"""

    def test_create_reads_account_id_property(self):
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "read-account-id"
            AssetFactory.create(config)

            mock_account_id.assert_called()

    def test_create_uses_current_account_id_at_call_time(self):
        """Verifies that account_id is read at create() time, not cached"""
        config = AssetConfig(asset_name="test-asset", data=b"test data")

        with patch.object(AuthenticationManager, 'account_id', new_callable=PropertyMock) as mock_account_id:
            mock_account_id.return_value = "first-account"
            result1 = AssetFactory.create(config)

            mock_account_id.return_value = "second-account"
            result2 = AssetFactory.create(config)

        assert result1.account_id == "first-account"
        assert result2.account_id == "second-account"