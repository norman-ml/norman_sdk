from typing import Any, AsyncGenerator, Coroutine

import pytest

from norman.objects.handlers.response_handler import ResponseHandler


def create_response_coroutine(
    chunks: list[bytes],
) -> Coroutine[Any, Any, tuple[dict, AsyncGenerator[bytes, None]]]:
    async def mock_stream() -> AsyncGenerator[bytes, None]:
        for chunk in chunks:
            yield chunk

    async def mock_coroutine() -> tuple[dict, AsyncGenerator[bytes, None]]:
        return {}, mock_stream()

    return mock_coroutine()

class TestResponseHandler:
    @pytest.mark.unit
    @pytest.mark.handler
    @pytest.mark.asyncio
    async def test_bytes_returns_bytearray(self) -> None:
        response_handler = ResponseHandler(create_response_coroutine([b"model output"]))

        result = await response_handler.bytes()

        assert isinstance(result, bytearray)
        assert result == b"model output"

    @pytest.mark.unit
    @pytest.mark.handler
    @pytest.mark.asyncio
    async def test_bytes_concatenates_multiple_chunks(self) -> None:
        response_handler = ResponseHandler(create_response_coroutine([b"hello ", b"world"]))

        result = await response_handler.bytes()

        assert result == b"hello world"

    @pytest.mark.unit
    @pytest.mark.handler
    @pytest.mark.asyncio
    async def test_bytes_returns_empty_for_empty_stream(self) -> None:
        response_handler = ResponseHandler(create_response_coroutine([]))

        result = await response_handler.bytes()

        assert result == b""

    @pytest.mark.asyncio
    async def test_stream_returns_async_iterable(self) -> None:
        response_handler = ResponseHandler(create_response_coroutine([b"chunk_1", b"chunk_2"]))

        stream = await response_handler.stream()
        collected_chunks = [chunk async for chunk in stream]

        assert collected_chunks == [b"chunk_1", b"chunk_2"]

    @pytest.mark.asyncio
    async def test_stream_preserves_chunk_boundaries(self) -> None:
        response_handler = ResponseHandler(create_response_coroutine([b"first", b"second"]))

        stream = await response_handler.stream()
        chunks = [chunk async for chunk in stream]

        assert len(chunks) == 2
        assert chunks[0] == b"first"
        assert chunks[1] == b"second"
