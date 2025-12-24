import pytest
from pathlib import Path
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

@pytest.fixture
def sample_asset_config():
    return AssetConfig(asset_name="sample-asset", data=b"sample binary data", source=InputSource.Primitive)

@pytest.mark.usefixtures("mock_account_id")
class TestAssetFactoryCreate:
    def test_create_returns_model_asset_instance(self, sample_asset_config: AssetConfig) -> None:
        created_asset = AssetFactory.create(sample_asset_config)

        assert isinstance(created_asset, ModelAsset)

    def test_create_sets_asset_name_from_config(self) -> None:
        audio_asset_config = AssetConfig(asset_name="audio-processor", data=b"audio data", source=InputSource.Primitive)
        created_asset = AssetFactory.create(audio_asset_config)

        assert created_asset.asset_name == "audio-processor"

    def test_create_sets_account_id_from_authentication_manager(self, sample_asset_config: AssetConfig) -> None:
        created_asset = AssetFactory.create(sample_asset_config)

        assert created_asset.account_id == "account-123"

    def test_create_sets_default_id(self, sample_asset_config: AssetConfig) -> None:
        created_asset = AssetFactory.create(sample_asset_config)

        assert created_asset.id == "0"

    def test_create_sets_default_model_id(self, sample_asset_config: AssetConfig) -> None:
        created_asset = AssetFactory.create(sample_asset_config)

        assert created_asset.model_id == "0"

    def test_create_sets_default_version_id(self, sample_asset_config: AssetConfig) -> None:
        created_asset = AssetFactory.create(sample_asset_config)

        assert created_asset.version_id == "0"

    def test_create_preserves_asset_name_with_special_characters(self) -> None:
        versioned_asset_config = AssetConfig(
            asset_name="model-weights_v2.1", data=b"weights data", source=InputSource.Primitive)
        created_asset = AssetFactory.create(versioned_asset_config)

        assert created_asset.asset_name == "model-weights_v2.1"

    def test_create_preserves_asset_name_with_spaces(self) -> None:
        spaced_asset_config = AssetConfig(asset_name="training data file", data=b"training data", source=InputSource.Primitive)
        created_asset = AssetFactory.create(spaced_asset_config)

        assert created_asset.asset_name == "training data file"

    def test_create_with_bytes_data(self) -> None:
        binary_asset_config = AssetConfig(asset_name="binary-model", data=b"\x00\x01\x02\x03", source=InputSource.Primitive)
        created_asset = AssetFactory.create(binary_asset_config)

        assert created_asset.asset_name == "binary-model"

    def test_create_with_string_data(self) -> None:
        text_asset_config = AssetConfig(asset_name="text-config", data="configuration content", source=InputSource.Primitive)
        created_asset = AssetFactory.create(text_asset_config)

        assert created_asset.asset_name == "text-config"

    def test_create_with_path_data_and_file_source(self) -> None:
        path_asset_config = AssetConfig(asset_name="external-weights", data=Path("/models/weights.pt"), source=InputSource.File)
        created_asset = AssetFactory.create(path_asset_config)

        assert created_asset.asset_name == "external-weights"

    def test_create_with_link_source(self) -> None:
        link_asset_config = AssetConfig(asset_name="remote-model", data="https://example.com/model.pt", source=InputSource.Link)
        created_asset = AssetFactory.create(link_asset_config)

        assert created_asset.asset_name == "remote-model"

    def test_create_with_stream_source(self) -> None:
        stream_asset_config = AssetConfig(asset_name="streamed-data", data=b"streaming content", source=InputSource.Stream)
        created_asset = AssetFactory.create(stream_asset_config)

        assert created_asset.asset_name == "streamed-data"

    def test_create_with_none_source(self) -> None:
        asset_config_without_source = AssetConfig(asset_name="no-source-asset", data=b"data without source", source=None)
        created_asset = AssetFactory.create(asset_config_without_source)

        assert created_asset.asset_name == "no-source-asset"

    def test_create_with_default_source(self) -> None:
        asset_config_default_source = AssetConfig(asset_name="default-source-asset", data=b"data with default source")
        created_asset = AssetFactory.create(asset_config_default_source)

        assert created_asset.asset_name == "default-source-asset"

    def test_create_multiple_assets_returns_independent_results(self) -> None:
        first_asset_config = AssetConfig(asset_name="encoder-weights", data=b"encoder data", source=InputSource.Primitive)
        second_asset_config = AssetConfig(asset_name="decoder-weights", data=b"decoder data", source=InputSource.Primitive)

        first_asset = AssetFactory.create(first_asset_config)
        second_asset = AssetFactory.create(second_asset_config)

        assert first_asset.asset_name == "encoder-weights"
        assert second_asset.asset_name == "decoder-weights"
        assert first_asset.account_id == second_asset.account_id == "account-123"
