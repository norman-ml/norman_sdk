import pytest
from unittest.mock import patch

from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.parameters.data_modality import DataModality
from norman_objects.shared.parameters.model_param import ModelParam

from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman.objects.factories.signature_factory import SignatureFactory


SIGNATURE_MODALITY_RESOLVER_PATH = "norman.objects.factories.signature_factory.SignatureModalityResolver"
PARAMETER_FACTORY_PATH = "norman.objects.factories.signature_factory.ParameterFactory"
AUDIO_INPUT = "Audio Input"

@pytest.fixture
def mock_signature_modality_resolver():
    with patch(SIGNATURE_MODALITY_RESOLVER_PATH) as mock:
        mock.resolve.return_value = DataModality.Audio
        yield mock


@pytest.fixture
def mock_parameter_factory():
    with patch(PARAMETER_FACTORY_PATH) as mock:
        yield mock


@pytest.fixture
def sample_signature_config() -> SignatureConfig:
    return SignatureConfig(
        display_title=AUDIO_INPUT,
        data_modality="audio",
        data_domain="speech",
        data_encoding="wav",
        receive_format=ReceiveFormat.Primitive,
        http_location=HttpLocation.Body,
        hidden=False,
        default_value=None,
        parameters=[]
    )


@pytest.fixture
def sample_parameter_config() -> ParameterConfig:
    return ParameterConfig(
        parameter_name="audio_waveform",
        data_encoding="wav"
    )


def create_mock_param(name: str = "audio_waveform") -> ModelParam:
    return ModelParam(
        id="0",
        model_id="0",
        version_id="0",
        signature_id="0",
        data_modality=DataModality.Audio,
        data_encoding="wav",
        parameter_name=name
    )


@pytest.mark.usefixtures("mock_signature_modality_resolver", "mock_parameter_factory")
class TestSignatureFactory:

    # --- Return Type and Signature Type ---

    def test_create_returns_model_signature_instance(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert isinstance(created_signature, ModelSignature)

    def test_create_sets_signature_type_input(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert created_signature.signature_type == SignatureType.Input

    def test_create_sets_signature_type_output(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Output)

        assert created_signature.signature_type == SignatureType.Output

    # --- Basic Field Mapping ---

    def test_create_sets_display_title_from_config(self) -> None:
        text_signature_config = SignatureConfig(
            display_title="Transcription Output",
            data_modality="text",
            data_domain="transcript",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(text_signature_config, SignatureType.Output)

        assert created_signature.display_title == "Transcription Output"

    def test_create_sets_data_domain_from_config(self) -> None:
        music_signature_config = SignatureConfig(
            display_title="Music Input",
            data_modality="audio",
            data_domain="music",
            data_encoding="mp3",
            receive_format=ReceiveFormat.File,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(music_signature_config, SignatureType.Input)

        assert created_signature.data_domain == "music"

    def test_create_sets_data_encoding_from_config(self) -> None:
        mp3_signature_config = SignatureConfig(
            display_title=AUDIO_INPUT,
            data_modality="audio",
            data_domain="speech",
            data_encoding="mp3",
            receive_format=ReceiveFormat.File,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(mp3_signature_config, SignatureType.Input)

        assert created_signature.data_encoding == "mp3"

    def test_create_sets_receive_format_from_config(self) -> None:
        file_receive_config = SignatureConfig(
            display_title="Image Input",
            data_modality="image",
            data_domain="photo",
            data_encoding="png",
            receive_format=ReceiveFormat.File,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(file_receive_config, SignatureType.Input)

        assert created_signature.receive_format == ReceiveFormat.File

    # --- Data Modality Resolution ---

    def test_create_calls_resolver_with_data_encoding(self) -> None:
        wav_signature_config = SignatureConfig(
            display_title=AUDIO_INPUT,
            data_modality="audio",
            data_domain="speech",
            data_encoding="wav",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        with patch(SIGNATURE_MODALITY_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Audio

            SignatureFactory.create(wav_signature_config, SignatureType.Input)

            mock_resolver.resolve.assert_called_once_with("wav")

    def test_create_sets_data_modality_from_resolver(self) -> None:
        video_signature_config = SignatureConfig(
            display_title="Video Input",
            data_modality="video",
            data_domain="stream",
            data_encoding="mp4",
            receive_format=ReceiveFormat.File,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        with patch(SIGNATURE_MODALITY_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.return_value = DataModality.Video

            created_signature = SignatureFactory.create(video_signature_config, SignatureType.Input)

            assert created_signature.data_modality == DataModality.Video

    # --- HTTP Location Handling ---

    def test_create_with_none_http_location_defaults_to_body(self) -> None:
        no_location_config = SignatureConfig(
            display_title=AUDIO_INPUT,
            data_modality="audio",
            data_domain="speech",
            data_encoding="wav",
            receive_format=ReceiveFormat.Primitive,
            http_location=None,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(no_location_config, SignatureType.Input)

        assert created_signature.http_location == HttpLocation.Body

    def test_create_preserves_explicit_http_location_path(self) -> None:
        path_location_config = SignatureConfig(
            display_title="Resource ID",
            data_modality="text",
            data_domain="identifier",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Path,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(path_location_config, SignatureType.Input)

        assert created_signature.http_location == HttpLocation.Path

    def test_create_preserves_explicit_http_location_query(self) -> None:
        query_location_config = SignatureConfig(
            display_title="Search Query",
            data_modality="text",
            data_domain="query",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Query,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(query_location_config, SignatureType.Input)

        assert created_signature.http_location == HttpLocation.Query

    # --- Hidden Handling ---

    def test_create_with_none_hidden_defaults_to_false(self) -> None:
        no_hidden_config = SignatureConfig(
            display_title=AUDIO_INPUT,
            data_modality="audio",
            data_domain="speech",
            data_encoding="wav",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=None,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(no_hidden_config, SignatureType.Input)

        assert created_signature.hidden is False

    def test_create_preserves_hidden_true(self) -> None:
        hidden_config = SignatureConfig(
            display_title="Internal Parameter",
            data_modality="text",
            data_domain="config",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=True,
            default_value=None,
            parameters=[]
        )

        created_signature = SignatureFactory.create(hidden_config, SignatureType.Input)

        assert created_signature.hidden is True

    # --- Default Value Handling ---

    def test_create_with_none_default_value(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert created_signature.default_value is None

    def test_create_preserves_default_value(self) -> None:
        default_value_config = SignatureConfig(
            display_title="Language Selection",
            data_modality="text",
            data_domain="language",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Query,
            hidden=False,
            default_value="en-US",
            parameters=[]
        )

        created_signature = SignatureFactory.create(default_value_config, SignatureType.Input)

        assert created_signature.default_value == "en-US"

    def test_create_preserves_empty_default_value(self) -> None:
        empty_default_config = SignatureConfig(
            display_title="Optional Input",
            data_modality="text",
            data_domain="optional",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value="",
            parameters=[]
        )

        created_signature = SignatureFactory.create(empty_default_config, SignatureType.Input)

        assert created_signature.default_value == ""

    # --- Parameters Factory Integration ---

    def test_create_with_no_parameters(self, sample_signature_config: SignatureConfig) -> None:
        with patch(PARAMETER_FACTORY_PATH) as mock_factory:
            created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

            assert created_signature.parameters == []
            mock_factory.create.assert_not_called()

    def test_create_calls_parameter_factory_for_parameter(self, sample_parameter_config: ParameterConfig) -> None:
        single_param_config = SignatureConfig(
            display_title=AUDIO_INPUT,
            data_modality="audio",
            data_domain="speech",
            data_encoding="wav",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[sample_parameter_config]
        )

        with patch(PARAMETER_FACTORY_PATH) as mock_factory:
            mock_param = create_mock_param("audio_waveform")
            mock_factory.create.return_value = mock_param

            created_signature = SignatureFactory.create(single_param_config, SignatureType.Input)

            mock_factory.create.assert_called_once_with(sample_parameter_config)
            assert len(created_signature.parameters) == 1
            assert created_signature.parameters[0] == mock_param

    def test_create_calls_parameter_factory_for_each_parameter(self) -> None:
        first_param_config = ParameterConfig(parameter_name="audio_channel_left", data_encoding="wav")
        second_param_config = ParameterConfig(parameter_name="audio_channel_right", data_encoding="wav")
        multi_param_config = SignatureConfig(
            display_title="Stereo Audio Input",
            data_modality="audio",
            data_domain="stereo",
            data_encoding="wav",
            receive_format=ReceiveFormat.File,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[first_param_config, second_param_config]
        )

        with patch(PARAMETER_FACTORY_PATH) as mock_factory:
            mock_params = [
                create_mock_param("audio_channel_left"),
                create_mock_param("audio_channel_right")
            ]
            mock_factory.create.side_effect = mock_params

            created_signature = SignatureFactory.create(multi_param_config, SignatureType.Input)

            assert mock_factory.create.call_count == 2
            assert len(created_signature.parameters) == 2

    # --- Fixed Values ---

    def test_create_sets_transforms_to_empty_list(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert created_signature.transforms == []

    def test_create_sets_signature_args_to_empty_dict(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert created_signature.signature_args == {}

    # --- Default ID Values ---

    def test_create_sets_default_id(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert created_signature.id == "0"

    def test_create_sets_default_model_id(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert created_signature.model_id == "0"

    def test_create_sets_default_version_id(self, sample_signature_config: SignatureConfig) -> None:
        created_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)

        assert created_signature.version_id == "0"

    # --- Error Propagation ---

    def test_create_propagates_resolver_error(self) -> None:
        unknown_encoding_config = SignatureConfig(
            display_title="Unknown Input",
            data_modality="unknown",
            data_domain="unknown",
            data_encoding="xyz123",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[]
        )

        with patch(SIGNATURE_MODALITY_RESOLVER_PATH) as mock_resolver:
            mock_resolver.resolve.side_effect = ValueError("Unknown signature encoding: xyz123")

            with pytest.raises(ValueError, match="Unknown signature encoding"):
                SignatureFactory.create(unknown_encoding_config, SignatureType.Input)

    def test_create_propagates_parameter_factory_error(self, sample_parameter_config: ParameterConfig) -> None:
        error_param_config = SignatureConfig(
            display_title="Error Input",
            data_modality="audio",
            data_domain="speech",
            data_encoding="wav",
            receive_format=ReceiveFormat.Primitive,
            http_location=HttpLocation.Body,
            hidden=False,
            default_value=None,
            parameters=[sample_parameter_config]
        )

        with patch(PARAMETER_FACTORY_PATH) as mock_factory:
            mock_factory.create.side_effect = ValueError("Invalid parameter configuration")

            with pytest.raises(ValueError, match="Invalid parameter configuration"):
                SignatureFactory.create(error_param_config, SignatureType.Input)

    # --- Multiple Calls ---

    def test_create_multiple_signatures_returns_independent_results(self, sample_signature_config: SignatureConfig) -> None:
        input_signature = SignatureFactory.create(sample_signature_config, SignatureType.Input)
        output_signature = SignatureFactory.create(sample_signature_config, SignatureType.Output)

        assert input_signature.signature_type == SignatureType.Input
        assert output_signature.signature_type == SignatureType.Output