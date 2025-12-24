import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from norman_objects.shared.security.sensitive import Sensitive
from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

from norman.resolvers.flag_status_resolver import FlagStatusResolver


def create_mock_status_flag(flag_value: StatusFlagValue):
    """Helper to create a mock StatusFlag"""
    mock_flag = MagicMock()
    mock_flag.flag_value = flag_value
    return mock_flag


class TestFlagStatusResolverWaitForEntities:
    """Tests for FlagStatusResolver.wait_for_entities() method"""

    # --- Success Cases ---

    @pytest.mark.asyncio
    async def test_wait_for_entities_returns_when_all_flags_finished(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1", "entity-2"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)],
            "entity-2": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            await resolver.wait_for_entities(token, entity_ids)

    @pytest.mark.asyncio
    async def test_wait_for_entities_returns_immediately_when_already_finished(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            await resolver.wait_for_entities(token, entity_ids)

            assert mock_persist.status_flags.get_status_flags.call_count == 1

    @pytest.mark.asyncio
    async def test_wait_for_entities_with_single_entity(self):
        token = Sensitive("test-token")
        entity_ids = ["single-entity"]

        mock_flags = {
            "single-entity": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            await resolver.wait_for_entities(token, entity_ids)

    @pytest.mark.asyncio
    async def test_wait_for_entities_with_multiple_flags_per_entity(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags = {
            "entity-1": [
                create_mock_status_flag(StatusFlagValue.Finished),
                create_mock_status_flag(StatusFlagValue.Finished),
                create_mock_status_flag(StatusFlagValue.Finished)
            ]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            await resolver.wait_for_entities(token, entity_ids)

    # --- Polling Behavior ---

    @pytest.mark.asyncio
    async def test_wait_for_entities_polls_until_finished(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags_in_progress = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.In_Progress)]
        }
        mock_flags_finished = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
             patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:

            mock_config.flag_timeout_seconds = 60
            mock_config.get_flags_interval = 1

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(
                side_effect=[mock_flags_in_progress, mock_flags_finished]
            )
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            resolver._timeout_seconds = 60

            await resolver.wait_for_entities(token, entity_ids)

            assert mock_persist.status_flags.get_status_flags.call_count == 2

    @pytest.mark.asyncio
    async def test_wait_for_entities_handles_not_started_state(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags_not_started = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Not_Started)]
        }
        mock_flags_finished = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
             patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:

            mock_config.flag_timeout_seconds = 60
            mock_config.get_flags_interval = 1

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(
                side_effect=[mock_flags_not_started, mock_flags_finished]
            )
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            resolver._timeout_seconds = 60

            await resolver.wait_for_entities(token, entity_ids)

            assert mock_persist.status_flags.get_status_flags.call_count == 2

    @pytest.mark.asyncio
    async def test_wait_for_entities_handles_enqueued_state(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags_enqueued = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Enqueued)]
        }
        mock_flags_finished = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
             patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:

            mock_config.flag_timeout_seconds = 60
            mock_config.get_flags_interval = 1

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(
                side_effect=[mock_flags_enqueued, mock_flags_finished]
            )
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            resolver._timeout_seconds = 60

            await resolver.wait_for_entities(token, entity_ids)

            assert mock_persist.status_flags.get_status_flags.call_count == 2

    # --- Error Cases ---

    @pytest.mark.asyncio
    async def test_wait_for_entities_raises_on_error_flag(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Error)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()

            with pytest.raises(ValueError, match="Status flags at error state"):
                await resolver.wait_for_entities(token, entity_ids)

    @pytest.mark.asyncio
    async def test_wait_for_entities_raises_on_error_flag_among_finished(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1", "entity-2"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)],
            "entity-2": [create_mock_status_flag(StatusFlagValue.Error)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()

            with pytest.raises(ValueError, match="Status flags at error state"):
                await resolver.wait_for_entities(token, entity_ids)

    @pytest.mark.asyncio
    async def test_wait_for_entities_raises_on_none_status_flags(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=None)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()

            with pytest.raises(ValueError, match="No status flags found"):
                await resolver.wait_for_entities(token, entity_ids)

    @pytest.mark.asyncio
    async def test_wait_for_entities_raises_timeout_error(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.In_Progress)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
                patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
                patch('norman.resolvers.flag_status_resolver.time') as mock_time, \
                patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:
            mock_config.get_flags_interval = 1

            # Need enough values for: wait_start_time, wait_end_time calculation,
            # while loop check, iteration_start_time, loop_iteration_end,
            # then while loop check again (which should exceed timeout)
            mock_time.time.side_effect = [
                0,  # wait_start_time
                0,  # while time.time() < wait_end_time (first check)
                0,  # iteration_start_time
                0,  # loop_iteration_end
                100,  # while time.time() < wait_end_time (second check - exceeds timeout)
            ]

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            resolver._timeout_seconds = 10

            with pytest.raises(TimeoutError, match="Timed out waiting for entities"):
                await resolver.wait_for_entities(token, entity_ids)

    # --- Query Constraints ---

    @pytest.mark.asyncio
    async def test_wait_for_entities_creates_correct_constraints(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1", "entity-2", "entity-3"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.QueryConstraints') as mock_constraints:

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist
            mock_constraints.includes.return_value = MagicMock()

            resolver = FlagStatusResolver()
            await resolver.wait_for_entities(token, entity_ids)

            mock_constraints.includes.assert_called_once_with(
                "Status_Flags",
                "Entity_ID",
                ["entity-1", "entity-2", "entity-3"]
            )

    @pytest.mark.asyncio
    async def test_wait_for_entities_passes_token_to_persist_service(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            await resolver.wait_for_entities(token, entity_ids)

            call_args = mock_persist.status_flags.get_status_flags.call_args
            assert call_args[0][0] == token

    # --- Sleep Behavior ---

    @pytest.mark.asyncio
    async def test_wait_for_entities_sleeps_between_polls(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags_in_progress = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.In_Progress)]
        }
        mock_flags_finished = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
             patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:

            mock_config.flag_timeout_seconds = 60
            mock_config.get_flags_interval = 5

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(
                side_effect=[mock_flags_in_progress, mock_flags_finished]
            )
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            resolver._timeout_seconds = 60

            await resolver.wait_for_entities(token, entity_ids)

            assert mock_sleep.called

    @pytest.mark.asyncio
    async def test_wait_for_entities_does_not_sleep_when_finished_immediately(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(return_value=mock_flags)
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            await resolver.wait_for_entities(token, entity_ids)

            mock_sleep.assert_not_called()


class TestFlagStatusResolverInit:
    """Tests for FlagStatusResolver initialization"""

    def test_init_creates_persist_service(self):
        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            resolver = FlagStatusResolver()

            mock_persist_class.assert_called_once()

    def test_init_sets_timeout_from_config(self):
        with patch('norman.resolvers.flag_status_resolver.Persist'), \
             patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:
            mock_config.flag_timeout_seconds = 3600

            resolver = FlagStatusResolver()

            assert resolver._timeout_seconds == 3600


class TestFlagStatusResolverEdgeCases:
    """Edge case tests for FlagStatusResolver"""

    @pytest.mark.asyncio
    async def test_wait_for_entities_with_empty_entity_list_raises_error(self):
        """Empty entity list raises ValueError from QueryConstraints"""
        token = Sensitive("test-token")
        entity_ids = []

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class:
            mock_persist = MagicMock()
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()

            with pytest.raises(ValueError, match="empty collection"):
                await resolver.wait_for_entities(token, entity_ids)

    @pytest.mark.asyncio
    async def test_wait_for_entities_with_mixed_flag_states_waits(self):
        token = Sensitive("test-token")
        entity_ids = ["entity-1", "entity-2"]

        mock_flags_mixed = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)],
            "entity-2": [create_mock_status_flag(StatusFlagValue.In_Progress)]
        }
        mock_flags_all_finished = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Finished)],
            "entity-2": [create_mock_status_flag(StatusFlagValue.Finished)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
             patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:

            mock_config.flag_timeout_seconds = 60
            mock_config.get_flags_interval = 1

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(
                side_effect=[mock_flags_mixed, mock_flags_all_finished]
            )
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            resolver._timeout_seconds = 60

            await resolver.wait_for_entities(token, entity_ids)

            assert mock_persist.status_flags.get_status_flags.call_count == 2

    @pytest.mark.asyncio
    async def test_wait_for_entities_error_on_second_poll(self):
        """Error can occur on any poll, not just the first"""
        token = Sensitive("test-token")
        entity_ids = ["entity-1"]

        mock_flags_in_progress = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.In_Progress)]
        }
        mock_flags_error = {
            "entity-1": [create_mock_status_flag(StatusFlagValue.Error)]
        }

        with patch('norman.resolvers.flag_status_resolver.Persist') as mock_persist_class, \
             patch('norman.resolvers.flag_status_resolver.asyncio.sleep', new_callable=AsyncMock) as mock_sleep, \
             patch('norman.resolvers.flag_status_resolver.NormanAppConfig') as mock_config:

            mock_config.flag_timeout_seconds = 60
            mock_config.get_flags_interval = 1

            mock_persist = MagicMock()
            mock_persist.status_flags.get_status_flags = AsyncMock(
                side_effect=[mock_flags_in_progress, mock_flags_error]
            )
            mock_persist_class.return_value = mock_persist

            resolver = FlagStatusResolver()
            resolver._timeout_seconds = 60

            with pytest.raises(ValueError, match="Status flags at error state"):
                await resolver.wait_for_entities(token, entity_ids)