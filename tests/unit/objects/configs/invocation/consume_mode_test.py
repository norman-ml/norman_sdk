import pytest

from norman.objects.configs.invocation.consume_mode import ConsumeMode


class TestConsumeMode:
    @pytest.mark.unit
    @pytest.mark.config
    def test_bytes_mode_has_correct_value(self) -> None:
        assert ConsumeMode.Bytes.value == "bytes"

    @pytest.mark.unit
    @pytest.mark.config
    def test_stream_mode_has_correct_value(self) -> None:
        assert ConsumeMode.Stream.value == "stream"
