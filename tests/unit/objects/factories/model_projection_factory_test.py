from unittest.mock import patch, PropertyMock, MagicMock

import pytest
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_projection import ModelProjection
from norman_objects.shared.models.model_tag import ModelTag
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.model_version import ModelVersion
from norman_objects.shared.models.output_format import OutputFormat

from norman.managers.authentication_manager import AuthenticationManager
from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.factories.model_projection_factory import ModelProjectionFactory

MODEL_VERSION_FACTORY_PATH = "norman.objects.factories.model_projection_factory.ModelVersionFactory"
TAG_FACTORY_PATH = "norman.objects.factories.model_projection_factory.TagFactory"

@pytest.fixture
def mock_account_id():
    with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock:
        mock.return_value = "account-123"
        yield mock


@pytest.fixture
def mock_version_factory():
    with patch(MODEL_VERSION_FACTORY_PATH) as mock:
        mock_version = MagicMock(spec=ModelVersion)
        mock_version.id = "0"
        mock_version.account_id = "account-123"
        mock_version.label = "v1.0"
        mock.create.return_value = mock_version
        yield mock


@pytest.fixture
def mock_tag_factory():
    with patch(TAG_FACTORY_PATH) as mock:
        yield mock


@pytest.fixture
def sample_version_config() -> ModelVersionConfig:
    return ModelVersionConfig(
        label="v1.0",
        short_description="Image classification model",
        long_description="A convolutional neural network for classifying images into categories",
        assets=[],
        inputs=[],
        outputs=[],
        hosting_location=ModelHostingLocation.Internal,
        model_type=ModelType.Pytorch_jit,
        request_type=HttpRequestType.Post,
        url=None,
        output_format=OutputFormat.Json,
        http_headers=None
    )

@pytest.fixture
def sample_projection_config(sample_version_config: ModelVersionConfig) -> ModelProjectionConfig:
    return ModelProjectionConfig(
        name="image-classifier",
        category="computer-vision",
        version=sample_version_config,
        user_tags= None
    )


def create_mock_model_tag(tag_name: str) -> MagicMock:
    mock_tag = MagicMock(spec=ModelTag)
    mock_tag.id = "0"
    mock_tag.account_id = "account-123"
    mock_tag.name = tag_name
    return mock_tag


@pytest.mark.usefixtures("mock_account_id", "mock_version_factory", "mock_tag_factory")
class TestModelProjectionFactory:
    # --- Return Type and Basic Fields ---

    def test_create_returns_model_projection_instance(self, sample_projection_config: ModelProjectionConfig) -> None:
        created_projection = ModelProjectionFactory.create(sample_projection_config)

        assert isinstance(created_projection, ModelProjection)

    def test_create_sets_name_from_config(self, sample_version_config: ModelVersionConfig) -> None:
        sentiment_analyzer_config = ModelProjectionConfig(
            name="sentiment-analyzer",
            category="natural-language-processing",
            version=sample_version_config,
            user_tags=None
        )

        created_projection = ModelProjectionFactory.create(sentiment_analyzer_config)

        assert created_projection.name == "sentiment-analyzer"

    def test_create_sets_category_from_config(self, sample_version_config: ModelVersionConfig) -> None:
        object_detector_config = ModelProjectionConfig(
            name="yolo-detector",
            category="object-detection",
            version=sample_version_config,
            user_tags=None
        )

        created_projection = ModelProjectionFactory.create(object_detector_config)

        assert created_projection.category == "object-detection"

    def test_create_sets_account_id_from_authentication_manager(
        self,
        sample_version_config: ModelVersionConfig
    ) -> None:
        speech_recognizer_config = ModelProjectionConfig(
            name="speech-recognizer",
            category="audio-processing",
            version=sample_version_config,
            user_tags=None
        )

        with patch.object(AuthenticationManager, "account_id", new_callable=PropertyMock) as mock_account:
            mock_account.return_value = "account-456"

            created_projection = ModelProjectionFactory.create(speech_recognizer_config)

            assert created_projection.account_id == "account-456"

    def test_create_sets_invocation_count_to_zero(self, sample_projection_config: ModelProjectionConfig) -> None:
        created_projection = ModelProjectionFactory.create(sample_projection_config)

        assert created_projection.invocation_count == 0

    def test_create_sets_default_id(self, sample_projection_config: ModelProjectionConfig) -> None:
        created_projection = ModelProjectionFactory.create(sample_projection_config)

        assert created_projection.id == "0"

    # --- Category Handling ---

    def test_create_with_none_category_sets_empty_string(self, sample_version_config: ModelVersionConfig) -> None:
        uncategorized_model_config = ModelProjectionConfig(
            name="uncategorized-model",
            category="placeholder",
            version=sample_version_config,
            user_tags=None
        )
        uncategorized_model_config.category = None

        created_projection = ModelProjectionFactory.create(uncategorized_model_config)

        assert created_projection.category == ""

    def test_create_preserves_empty_category(self, sample_version_config: ModelVersionConfig) -> None:
        empty_category_config = ModelProjectionConfig(
            name="empty-category-model",
            category="",
            version=sample_version_config,
            user_tags=None
        )

        created_projection = ModelProjectionFactory.create(empty_category_config)

        assert created_projection.category == ""

    # --- Version Factory Integration ---

    def test_create_calls_model_version_factory(self, sample_version_config: ModelVersionConfig) -> None:
        transformer_model_config = ModelProjectionConfig(
            name="bert-encoder",
            category="transformers",
            version=sample_version_config,
            user_tags=None
        )

        with patch(MODEL_VERSION_FACTORY_PATH) as mock_factory:
            mock_version = MagicMock(spec=ModelVersion)
            mock_factory.create.return_value = mock_version

            ModelProjectionFactory.create(transformer_model_config)

            mock_factory.create.assert_called_once_with(sample_version_config)

    def test_create_sets_version_from_factory(self, sample_version_config: ModelVersionConfig) -> None:
        embedding_model_config = ModelProjectionConfig(
            name="word-embedder",
            category="embeddings",
            version=sample_version_config,
            user_tags=None
        )

        with patch(MODEL_VERSION_FACTORY_PATH) as mock_factory:
            mock_version = MagicMock(spec=ModelVersion)
            mock_version.label = "v2.0-beta"
            mock_factory.create.return_value = mock_version

            created_projection = ModelProjectionFactory.create(embedding_model_config)

            assert created_projection.version == mock_version

    # --- User Tags Handling ---

    def test_create_with_none_user_tags_returns_empty_list(self, sample_version_config: ModelVersionConfig) -> None:
        untagged_model_config = ModelProjectionConfig(
            name="untagged-model",
            category="general",
            version=sample_version_config,
            user_tags=None
        )

        with patch(TAG_FACTORY_PATH) as mock_factory:
            created_projection = ModelProjectionFactory.create(untagged_model_config)

            assert created_projection.user_tags == []
            mock_factory.create.assert_not_called()

    def test_create_with_empty_user_tags_list(self, sample_version_config: ModelVersionConfig) -> None:
        empty_tags_config = ModelProjectionConfig(
            name="empty-tags-model",
            category="general",
            version=sample_version_config,
            user_tags=[]
        )

        with patch(TAG_FACTORY_PATH) as mock_factory:
            created_projection = ModelProjectionFactory.create(empty_tags_config)

            assert created_projection.user_tags == []
            mock_factory.create.assert_not_called()

    def test_create_with_single_user_tag(self, sample_version_config: ModelVersionConfig) -> None:
        production_tag_config = ModelTagConfig(name="production")
        single_tag_model_config = ModelProjectionConfig(
            name="production-model",
            category="deployment",
            version=sample_version_config,
            user_tags=[production_tag_config]
        )

        with patch(TAG_FACTORY_PATH) as mock_factory:
            mock_production_tag = create_mock_model_tag("production")
            mock_factory.create.return_value = mock_production_tag

            created_projection = ModelProjectionFactory.create(single_tag_model_config)

            assert len(created_projection.user_tags) == 1
            assert created_projection.user_tags[0] == mock_production_tag
            mock_factory.create.assert_called_once_with(production_tag_config)

    def test_create_with_multiple_user_tags(self, sample_version_config: ModelVersionConfig) -> None:
        tag_configs = [
            ModelTagConfig(name="machine-learning"),
            ModelTagConfig(name="pytorch"),
            ModelTagConfig(name="experimental")
        ]
        multi_tag_model_config = ModelProjectionConfig(
            name="experimental-ml-model",
            category="research",
            version=sample_version_config,
            user_tags=tag_configs
        )

        with patch(TAG_FACTORY_PATH) as mock_factory:
            mock_tags = [
                create_mock_model_tag("machine-learning"),
                create_mock_model_tag("pytorch"),
                create_mock_model_tag("experimental")
            ]
            mock_factory.create.side_effect = mock_tags

            created_projection = ModelProjectionFactory.create(multi_tag_model_config)

            assert len(created_projection.user_tags) == 3
            assert mock_factory.create.call_count == 3

    def test_create_calls_tag_factory_for_each_tag(self, sample_version_config: ModelVersionConfig) -> None:
        nlp_tag_config = ModelTagConfig(name="nlp")
        transformers_tag_config = ModelTagConfig(name="transformers")
        tagged_model_config = ModelProjectionConfig(
            name="language-model",
            category="nlp",
            version=sample_version_config,
            user_tags=[nlp_tag_config, transformers_tag_config]
        )

        with patch(TAG_FACTORY_PATH) as mock_factory:
            mock_factory.create.side_effect = [
                create_mock_model_tag("nlp"),
                create_mock_model_tag("transformers")
            ]

            ModelProjectionFactory.create(tagged_model_config)

            mock_factory.create.assert_any_call(nlp_tag_config)
            mock_factory.create.assert_any_call(transformers_tag_config)

    # --- Name Variations ---

    def test_create_preserves_name_with_special_characters(self, sample_version_config: ModelVersionConfig) -> None:
        special_name_config = ModelProjectionConfig(
            name="resnet-50_v1.0",
            category="image-classification",
            version=sample_version_config,
            user_tags=None
        )

        created_projection = ModelProjectionFactory.create(special_name_config)

        assert created_projection.name == "resnet-50_v1.0"

    def test_create_preserves_name_with_spaces(self, sample_version_config: ModelVersionConfig) -> None:
        spaced_name_config = ModelProjectionConfig(
            name="My Custom Vision Model",
            category="computer-vision",
            version=sample_version_config,
            user_tags=None
        )

        created_projection = ModelProjectionFactory.create(spaced_name_config)

        assert created_projection.name == "My Custom Vision Model"
