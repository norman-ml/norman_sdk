import pytest
from typing import AsyncGenerator

from norman.objects.handlers.response_handler import ResponseHandler


def create_response_coroutine(chunks: list[bytes]):
    async def mock_stream() -> AsyncGenerator[bytes, None]:
        for chunk in chunks:
            yield chunk

    async def mock_coroutine():
        return {}, mock_stream()

    return mock_coroutine()

class TestResponseHandler:
    # --- Bytes ---
    @pytest.mark.asyncio
    async def test_bytes_returns_bytearray(self) -> None:
        handler = ResponseHandler(create_response_coroutine([b"model output"]))

        result = await handler.bytes()

        assert isinstance(result, bytearray)
        assert result == b"model output"

    @pytest.mark.asyncio
    async def test_bytes_concatenates_multiple_chunks(self) -> None:
        handler = ResponseHandler(create_response_coroutine([b"hello ", b"world"]))

        result = await handler.bytes()

        assert result == b"hello world"

    @pytest.mark.asyncio
    async def test_bytes_returns_empty_for_empty_stream(self) -> None:
        handler = ResponseHandler(create_response_coroutine([]))

        result = await handler.bytes()

        assert result == b""

    # --- Stream ---

    @pytest.mark.asyncio
    async def test_stream_returns_iterable(self) -> None:
        handler = ResponseHandler(create_response_coroutine([b"chunk_1", b"chunk_2"]))

        stream = await handler.stream()

        collected_chunks = [chunk async for chunk in stream]

        assert collected_chunks == [b"chunk_1", b"chunk_2"]

    @pytest.mark.asyncio
    async def test_stream_preserves_chunk_boundaries(self) -> None:
        handler = ResponseHandler(create_response_coroutine([b"first", b"second"]))

        stream = await handler.stream()

        chunks = [chunk async for chunk in stream]

        assert len(chunks) == 2
        assert chunks[0] == b"first"
        assert chunks[1] == b"second"
