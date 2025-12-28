from typing import Any, Coroutine, Tuple, AsyncIterator

import httpx


class ResponseHandler:
    def __init__(self, response_coroutine: Coroutine[Any, Any, Tuple[httpx.Headers, AsyncIterator[bytes]]]) -> None:
        self.__response_coroutine = response_coroutine

    async def bytes(self) -> bytes:
        bytes_result = bytearray()

        headers, byte_stream = await self.__response_coroutine
        async for chunk in byte_stream:
            bytes_result.extend(chunk)

        return bytes_result

    async def stream(self) -> AsyncIterator[bytes]:
        headers, byte_stream = await self.__response_coroutine
        return byte_stream