import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

from norman.resolvers.flag_status_resolver import FlagStatusResolver


PERSIST_PATH = "norman.resolvers.flag_status_resolver.Persist"
ASYNCIO_SLEEP_PATH = "norman.resolvers.flag_status_resolver.asyncio.sleep"
CONFIG_PATH = "norman.resolvers.flag_status_resolver.NormanAppConfig"
TIME_PATH = "norman.resolvers.flag_status_resolver.time"
QUERY_CONSTRAINTS_PATH = "norman.resolvers.flag_status_resolver.QueryConstraints"


def create_mock_status_flag(flag_value: StatusFlagValue) -> MagicMock:
    mock_flag = MagicMock()
    mock_flag.flag_value = flag_value
    return mock_flag


def create_mock_flags(entity_flags: dict[str, list[StatusFlagValue]]) -> dict[str, list[MagicMock]]:
    return {
        entity_id: [create_mock_status_flag(value) for value in values]
        for entity_id, values in entity_flags.items()
    }


def mock_status_flags(mock_persist: MagicMock, flags: dict[str, list[StatusFlagValue]]) -> None:
    mock_persist.status_flags.get_status_flags = AsyncMock(
        return_value=create_mock_flags(flags)
    )


def mock_status_flag_sequence(mock_persist: MagicMock, sequence: list[dict[str, list[StatusFlagValue]]]) -> None:
    mock_persist.status_flags.get_status_flags = AsyncMock(
        side_effect=[create_mock_flags(flags) for flags in sequence]
    )


@pytest.fixture
def mock_persist():
    with patch(PERSIST_PATH) as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_config():
    with patch(CONFIG_PATH) as mock:
        mock.flag_timeout_seconds = 60
        mock.get_flags_interval = 1
        yield mock


@pytest.fixture
def mock_sleep():
    with patch(ASYNCIO_SLEEP_PATH, new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def sample_token() -> Sensitive:
    return Sensitive("test-token")


@pytest.fixture
def resolver(mock_config) -> FlagStatusResolver:
    resolver = FlagStatusResolver()
    resolver._timeout_seconds = 60
    return resolver


class TestFlagStatusResolver:

    @pytest.mark.asyncio
    async def test_returns_when_all_finished(self, mock_persist, resolver, sample_token):
        mock_status_flags(mock_persist, {
            "model-version-1": [StatusFlagValue.Finished],
            "model-version-2": [StatusFlagValue.Finished]
        })

        await resolver.wait_for_entities(sample_token, ["model-version-1", "model-version-2"])

        assert mock_persist.status_flags.get_status_flags.call_count == 1

    @pytest.mark.asyncio
    async def test_polls_until_finished(self, mock_persist, mock_sleep, resolver, sample_token):
        mock_status_flag_sequence(mock_persist, [
            {"model-version-1": [StatusFlagValue.In_Progress]},
            {"model-version-1": [StatusFlagValue.Finished]}
        ])

        await resolver.wait_for_entities(sample_token, ["model-version-1"])

        assert mock_persist.status_flags.get_status_flags.call_count == 2
        assert mock_sleep.called

    @pytest.mark.asyncio
    async def test_raises_on_error_flag(self, mock_persist, resolver, sample_token):
        mock_status_flags(mock_persist, {"model-version-1": [StatusFlagValue.Error]})

        with pytest.raises(ValueError, match="Status flags at error state"):
            await resolver.wait_for_entities(sample_token, ["model-version-1"])

    @pytest.mark.asyncio
    async def test_raises_on_none_response(self, mock_persist, resolver, sample_token):
        mock_persist.status_flags.get_status_flags = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="No status flags found"):
            await resolver.wait_for_entities(sample_token, ["model-version-1"])

    @pytest.mark.asyncio
    async def test_raises_on_empty_list(self, mock_persist, resolver, sample_token):
        with pytest.raises(ValueError, match="empty collection"):
            await resolver.wait_for_entities(sample_token, [])

    @pytest.mark.asyncio
    async def test_creates_correct_query_constraints(self, mock_persist, resolver, sample_token):
        mock_status_flags(mock_persist, {"model-version-1": [StatusFlagValue.Finished]})

        with patch(QUERY_CONSTRAINTS_PATH) as mock_constraints:
            mock_constraints.includes.return_value = MagicMock()

            await resolver.wait_for_entities(sample_token, ["model-version-1", "model-version-2"])

            mock_constraints.includes.assert_called_once_with(
                "Status_Flags",
                "Entity_ID",
                ["model-version-1", "model-version-2"]
            )