import pytest

from norman.objects.handlers.response_handler import ResponseHandler


class TestResponseHandlerBytes:
    """Tests for ResponseHandler.bytes() method"""

    @pytest.mark.asyncio
    async def test_bytes_returns_bytearray_type(self):
        async def mock_stream():
            yield b"hello"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert isinstance(result, bytearray)

    @pytest.mark.asyncio
    async def test_bytes_returns_single_chunk(self):
        async def mock_stream():
            yield b"hello world"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b"hello world"

    @pytest.mark.asyncio
    async def test_bytes_concatenates_multiple_chunks(self):
        async def mock_stream():
            yield b"hello "
            yield b"world"
            yield b"!"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b"hello world!"

    @pytest.mark.asyncio
    async def test_bytes_returns_empty_bytes_for_empty_stream(self):
        async def mock_stream():
            return
            yield  # Makes this a generator

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b""

    @pytest.mark.asyncio
    async def test_bytes_handles_large_chunks(self):
        large_chunk = b"x" * 10000

        async def mock_stream():
            yield large_chunk

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == large_chunk
        assert len(result) == 10000

    @pytest.mark.asyncio
    async def test_bytes_handles_many_small_chunks(self):
        async def mock_stream():
            for i in range(100):
                yield b"a"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b"a" * 100

    @pytest.mark.asyncio
    async def test_bytes_handles_binary_data(self):
        binary_data = bytes([0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD])

        async def mock_stream():
            yield binary_data

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == binary_data

    @pytest.mark.asyncio
    async def test_bytes_ignores_headers(self):
        headers = {"Content-Type": "application/octet-stream", "Content-Length": "5"}

        async def mock_stream():
            yield b"hello"

        async def mock_coroutine():
            return (headers, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b"hello"


class TestResponseHandlerStream:
    """Tests for ResponseHandler.stream() method"""

    @pytest.mark.asyncio
    async def test_stream_returns_stream_object(self):
        async def mock_stream():
            yield b"data"

        stream_gen = mock_stream()

        async def mock_coroutine():
            return ({}, stream_gen)

        handler = ResponseHandler(mock_coroutine())
        result = await handler.stream()

        assert result is stream_gen

    @pytest.mark.asyncio
    async def test_stream_can_be_iterated(self):
        async def mock_stream():
            yield b"chunk1"
            yield b"chunk2"
            yield b"chunk3"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        stream = await handler.stream()

        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == [b"chunk1", b"chunk2", b"chunk3"]

    @pytest.mark.asyncio
    async def test_stream_ignores_headers(self):
        headers = {"Content-Type": "text/plain"}

        async def mock_stream():
            yield b"data"

        async def mock_coroutine():
            return (headers, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        stream = await handler.stream()

        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == [b"data"]

    @pytest.mark.asyncio
    async def test_stream_returns_empty_stream(self):
        async def mock_stream():
            return
            yield

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        stream = await handler.stream()

        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == []


class TestResponseHandlerInit:
    @pytest.mark.asyncio
    async def test_coroutine_is_awaited_on_bytes(self):
        call_count = 0

        async def mock_stream():
            yield b"data"

        async def mock_coroutine():
            nonlocal call_count
            call_count += 1
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        await handler.bytes()

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_coroutine_is_awaited_on_stream(self):
        call_count = 0

        async def mock_stream():
            yield b"data"

        async def mock_coroutine():
            nonlocal call_count
            call_count += 1
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        await handler.stream()

        assert call_count == 1


class TestResponseHandlerEdgeCases:
    """Edge case tests for ResponseHandler"""

    @pytest.mark.asyncio
    async def test_bytes_with_mixed_chunk_sizes(self):
        async def mock_stream():
            yield b"a"
            yield b"bb"
            yield b"ccc"
            yield b"dddd"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b"abbcccdddd"

    @pytest.mark.asyncio
    async def test_bytes_with_empty_chunks_interspersed(self):
        async def mock_stream():
            yield b"hello"
            yield b""
            yield b"world"
            yield b""

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b"helloworld"

    @pytest.mark.asyncio
    async def test_stream_preserves_chunk_boundaries(self):
        async def mock_stream():
            yield b"first"
            yield b"second"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        stream = await handler.stream()

        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        # Chunks should be preserved separately, not concatenated
        assert chunks == [b"first", b"second"]
        assert chunks[0] == b"first"
        assert chunks[1] == b"second"

    @pytest.mark.asyncio
    async def test_bytes_only_consumes_stream_once(self):
        """Calling bytes() consumes the stream - it cannot be reused"""
        consumption_count = 0

        async def mock_stream():
            nonlocal consumption_count
            consumption_count += 1
            yield b"data"

        async def mock_coroutine():
            return ({}, mock_stream())

        handler = ResponseHandler(mock_coroutine())
        result = await handler.bytes()

        assert result == b"data"
        assert consumption_count == 1