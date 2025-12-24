from norman.objects.configs.invocation.consume_mode import ConsumeMode


class TestConsumeMode:

    def test_bytes_mode_has_correct_value(self) -> None:
        assert ConsumeMode.Bytes.value == "bytes"

    def test_stream_mode_has_correct_value(self) -> None:
        assert ConsumeMode.Stream.value == "stream"
