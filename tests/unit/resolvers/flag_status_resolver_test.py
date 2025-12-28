import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Generator

from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

from norman.resolvers.flag_status_resolver import FlagStatusResolver


def create_mock_status_flag(flag_value: StatusFlagValue) -> MagicMock:
    mock_flag = MagicMock()
    mock_flag.flag_value = flag_value
    return mock_flag


def create_mock_flags(entity_flags: dict[str, list[StatusFlagValue]]) -> dict[str, list[MagicMock]]:
    return {
        entity_id: [create_mock_status_flag(value) for value in values]
        for entity_id, values in entity_flags.items()
    }


def mock_status_flags(persist_mock: MagicMock, flags: dict[str, list[StatusFlagValue]]) -> None:
    persist_mock.status_flags.get_status_flags = AsyncMock(return_value=create_mock_flags(flags))


def mock_status_flag_sequence(persist_mock: MagicMock, sequence: list[dict[str, list[StatusFlagValue]]]) -> None:
    persist_mock.status_flags.get_status_flags = AsyncMock(side_effect=[create_mock_flags(flags) for flags in sequence])


@pytest.fixture(scope="function")
def persist_mock() -> Generator[MagicMock, None, None]:
    with patch("norman.resolvers.flag_status_resolver.Persist") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture(scope="function")
def config_mock() -> Generator[MagicMock, None, None]:
    with patch("norman.resolvers.flag_status_resolver.NormanAppConfig") as mock:
        mock.flag_timeout_seconds = 60
        mock.get_flags_interval = 1
        yield mock


@pytest.fixture(scope="function")
def sleep_mock() -> Generator[AsyncMock, None, None]:
    with patch("norman.resolvers.flag_status_resolver.asyncio.sleep", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture(scope="function")
def time_mock() -> Generator[MagicMock, None, None]:
    with patch("norman.resolvers.flag_status_resolver.time") as mock:
        mock.time.return_value = 0
        yield mock


@pytest.fixture(scope="function")
def sensitive_token() -> Sensitive:
    return Sensitive("test-token")


@pytest.fixture(scope="function")
def flag_status_resolver(config_mock: MagicMock) -> FlagStatusResolver:
    flag_status_resolver = FlagStatusResolver()
    flag_status_resolver._timeout_seconds = 60
    return flag_status_resolver


@pytest.mark.unit
class TestFlagStatusResolver:
    @pytest.mark.resolver
    async def test_returns_when_all_finished(self, persist_mock: MagicMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        entity_ids = ["model-version-1", "model-version-2"]
        mock_status_flags(persist_mock, {
            "model-version-1": [StatusFlagValue.Finished],
            "model-version-2": [StatusFlagValue.Finished],
        })

        await flag_status_resolver.wait_for_entities(sensitive_token, entity_ids)

        assert persist_mock.status_flags.get_status_flags.call_count == 1

    @pytest.mark.resolver
    async def test_polls_until_finished(self, persist_mock: MagicMock, sleep_mock: AsyncMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        entity_id = "model-version-1"
        mock_status_flag_sequence(persist_mock, [
            {entity_id: [StatusFlagValue.In_Progress]},
            {entity_id: [StatusFlagValue.Finished]},
        ])

        await flag_status_resolver.wait_for_entities(sensitive_token, [entity_id])

        assert persist_mock.status_flags.get_status_flags.call_count == 2
        assert sleep_mock.called

    @pytest.mark.resolver
    async def test_polls_multiple_cycles_until_finished(self, persist_mock: MagicMock, sleep_mock: AsyncMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        entity_id = "model-version-1"
        mock_status_flag_sequence(persist_mock, [
            {entity_id: [StatusFlagValue.In_Progress]},
            {entity_id: [StatusFlagValue.In_Progress]},
            {entity_id: [StatusFlagValue.In_Progress]},
            {entity_id: [StatusFlagValue.Finished]},
        ])

        await flag_status_resolver.wait_for_entities(sensitive_token, [entity_id])

        assert persist_mock.status_flags.get_status_flags.call_count == 4
        assert sleep_mock.call_count == 3

    @pytest.mark.resolver
    async def test_waits_for_all_entities_to_finish(self, persist_mock: MagicMock, sleep_mock: AsyncMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        mock_status_flag_sequence(persist_mock, [
            {"entity-1": [StatusFlagValue.Finished], "entity-2": [StatusFlagValue.In_Progress]},
            {"entity-1": [StatusFlagValue.Finished], "entity-2": [StatusFlagValue.Finished]},
        ])

        await flag_status_resolver.wait_for_entities(sensitive_token, ["entity-1", "entity-2"])

        assert persist_mock.status_flags.get_status_flags.call_count == 2

    @pytest.mark.resolver
    async def test_raises_on_error_flag(self, persist_mock: MagicMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        entity_id = "model-version-1"
        mock_status_flags(persist_mock, {entity_id: [StatusFlagValue.Error]})

        with pytest.raises(ValueError, match="Status flags at error state"):
            await flag_status_resolver.wait_for_entities(sensitive_token, [entity_id])

    @pytest.mark.resolver
    async def test_raises_on_error_flag_after_polling(self, persist_mock: MagicMock, sleep_mock: AsyncMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        entity_id = "model-version-1"
        mock_status_flag_sequence(persist_mock, [
            {entity_id: [StatusFlagValue.In_Progress]},
            {entity_id: [StatusFlagValue.Error]},
        ])

        with pytest.raises(ValueError, match="Status flags at error state"):
            await flag_status_resolver.wait_for_entities(sensitive_token, [entity_id])

    @pytest.mark.resolver
    async def test_raises_on_none_response(self, persist_mock: MagicMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        persist_mock.status_flags.get_status_flags = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="No status flags found"):
            await flag_status_resolver.wait_for_entities(sensitive_token, ["model-version-1"])

    @pytest.mark.resolver
    async def test_raises_on_empty_list(self, persist_mock: MagicMock, flag_status_resolver: FlagStatusResolver, sensitive_token: Sensitive) -> None:
        with pytest.raises(ValueError, match="empty collection"):
            await flag_status_resolver.wait_for_entities(sensitive_token, [])
