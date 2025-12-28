import pytest

from norman_objects.shared.inputs.input_source import InputSource

from norman.objects.configs.model.asset_config import AssetConfig
from tests.constants import SAMPLE_INPUTS_DIR, SAMPLE_INPUT_TXT


@pytest.mark.unit
@pytest.mark.config
class TestAssetConfig:
    def test_create_with_all_fields(self) -> None:
        asset_name = "model_weights.pt"
        data = b"binary weights"

        asset_config = AssetConfig(
            asset_name=asset_name,
            data=data,
            source=InputSource.Primitive,
        )

        assert asset_config.asset_name == asset_name
        assert asset_config.data == data
        assert asset_config.source == InputSource.Primitive

    def test_create_with_file_path(self) -> None:
        asset_name = "sample_text.txt"
        data = SAMPLE_INPUTS_DIR / SAMPLE_INPUT_TXT

        asset_config = AssetConfig(
            asset_name=asset_name,
            data=data,
        )

        assert asset_config.asset_name == asset_name
        assert asset_config.data == data
        assert asset_config.source is None

    def test_required_fields(self) -> None:
        required_fields = {
            name for name, field in AssetConfig.model_fields.items()
            if field.is_required()
        }

        assert required_fields == {"asset_name", "data"}
