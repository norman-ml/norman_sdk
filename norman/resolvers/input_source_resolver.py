import asyncio
from io import IOBase
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import huggingface_hub
from norman_objects.shared.inputs.input_source import InputSource


class InputSourceResolver:
    @staticmethod
    def resolve(data: Any) -> InputSource:
        if data is None:
            raise ValueError("Input data cannot be None")
        elif isinstance(data, Path):
            if data.exists():
                return InputSource.File

            raise FileNotFoundError("No file exists at the specified location")
        elif InputSourceResolver.__is_async_stream(data):
            return InputSource.Stream
        elif InputSourceResolver.__is_sync_stream(data):
            return InputSource.Stream
        elif isinstance(data, str):
            stripped = data.strip()

            if InputSourceResolver.__is_url(stripped):
                return InputSource.Link

            path = Path(stripped)
            if path.exists():
                return InputSource.File

            return InputSource.Primitive
        else:
            return InputSource.Primitive

    @staticmethod
    def __is_url(data: str):
        parsed = urlparse(data)
        if parsed.scheme not in ["http", "https"]:
            return False
        elif parsed.netloc is None or parsed.netloc == "":
            return False
        else:
            return True

    @staticmethod
    def __is_sync_stream(obj: Any) -> bool:
        if isinstance(obj, IOBase):
            return True

        read_attribute = getattr(obj, "read", None)
        if not callable(read_attribute):
            return False

        iter_attribute = getattr(obj, "__iter__", None)
        if not callable(iter_attribute):
            return False

        next_attribute = getattr(obj, "__next__", None)
        if not callable(next_attribute):
            return False

        return True

    @staticmethod
    def __is_async_stream(obj: Any) -> bool:
        aiter_attribute = getattr(obj, "__aiter__", None)
        if not callable(aiter_attribute):
            return False

        anext_attribute = getattr(obj, "__anext__", None)
        if not callable(anext_attribute):
            return False

        read_attribute = getattr(obj, "read", None)
        if not callable(read_attribute) or not asyncio.iscoroutinefunction(read_attribute):
            return False

        return True

    @staticmethod
    def __is_huggingface_model(obj: Any) -> bool:
        return huggingface_hub.repo_exists(obj)
