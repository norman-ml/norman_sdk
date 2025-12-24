from pathlib import Path

from norman_objects.shared.inputs.input_source import InputSource
from norman.objects.configs.model.asset_config import AssetConfig


class TestAssetConfig:

    def test_create_with_all_fields(self) -> None:
        config = AssetConfig(asset_name="model_weights.pt", data=b"binary weights", source=InputSource.Primitive)

        assert config.asset_name == "model_weights.pt"
        assert config.data == b"binary weights"
        assert config.source == InputSource.Primitive

    def test_create_without_optional_fields(self) -> None:
        config = AssetConfig(asset_name="config.json", data=Path("/models/config.json"))

        assert config.source is None

    def test_required_fields(self) -> None:
        required_fields = {name for name, field in AssetConfig.model_fields.items() if field.is_required()}

        assert required_fields == {"asset_name", "data"}
