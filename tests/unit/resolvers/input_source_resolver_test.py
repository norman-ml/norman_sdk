import pytest
from unittest.mock import MagicMock, AsyncMock
from pathlib import Path
from io import BytesIO, StringIO
import asyncio

from norman_objects.shared.inputs.input_source import InputSource

from norman.resolvers.input_source_resolver import InputSourceResolver

## TODO - SHOULD EMPTY STRING BE FILE OR PRIMITIVE ?

class TestInputSourceResolverResolve:
    """Tests for InputSourceResolver.resolve() method"""

    # --- None Input ---

    def test_resolve_none_raises_value_error(self):
        with pytest.raises(ValueError, match="Input data cannot be None"):
            InputSourceResolver.resolve(None)

    # --- Path Inputs ---

    def test_resolve_existing_path_returns_file(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        result = InputSourceResolver.resolve(test_file)

        assert result == InputSource.File

    def test_resolve_nonexistent_path_raises_file_not_found(self, tmp_path):
        nonexistent_path = tmp_path / "nonexistent.txt"

        with pytest.raises(FileNotFoundError, match="No file exists"):
            InputSourceResolver.resolve(nonexistent_path)

    def test_resolve_existing_directory_path_returns_file(self, tmp_path):
        # Directories also "exist", so they return File
        result = InputSourceResolver.resolve(tmp_path)

        assert result == InputSource.File

    # --- String URL Inputs ---

    def test_resolve_http_url_returns_link(self):
        result = InputSourceResolver.resolve("http://example.com/resource")

        assert result == InputSource.Link

    def test_resolve_https_url_returns_link(self):
        result = InputSourceResolver.resolve("https://example.com/resource")

        assert result == InputSource.Link

    def test_resolve_url_with_path_returns_link(self):
        result = InputSourceResolver.resolve("https://example.com/path/to/resource")

        assert result == InputSource.Link

    def test_resolve_url_with_query_params_returns_link(self):
        result = InputSourceResolver.resolve("https://example.com/resource?param=value")

        assert result == InputSource.Link

    def test_resolve_url_with_port_returns_link(self):
        result = InputSourceResolver.resolve("https://example.com:8080/resource")

        assert result == InputSource.Link

    def test_resolve_url_with_whitespace_returns_link(self):
        result = InputSourceResolver.resolve("  https://example.com/resource  ")

        assert result == InputSource.Link

    def test_resolve_ftp_url_returns_primitive(self):
        # Only http/https are recognized as links
        result = InputSourceResolver.resolve("ftp://example.com/file")

        assert result == InputSource.Primitive

    def test_resolve_url_without_netloc_returns_primitive(self):
        result = InputSourceResolver.resolve("http:///path")

        assert result == InputSource.Primitive

    # --- String File Path Inputs ---

    def test_resolve_string_existing_file_path_returns_file(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        result = InputSourceResolver.resolve(str(test_file))

        assert result == InputSource.File

    def test_resolve_string_existing_directory_returns_file(self, tmp_path):
        result = InputSourceResolver.resolve(str(tmp_path))

        assert result == InputSource.File

    def test_resolve_string_nonexistent_path_returns_primitive(self, tmp_path):
        # Non-existent string paths are treated as primitive strings
        nonexistent = str(tmp_path / "nonexistent.txt")

        result = InputSourceResolver.resolve(nonexistent)

        assert result == InputSource.Primitive

    # --- String Primitive Inputs ---

    def test_resolve_plain_string_returns_primitive(self):
        result = InputSourceResolver.resolve("hello world")

        assert result == InputSource.Primitive

    def test_resolve_empty_string_returns_file(self):
        """Empty string becomes Path('') which resolves to current directory"""
        result = InputSourceResolver.resolve("")

        # Empty string -> Path("") -> current directory -> exists -> File
        assert result == InputSource.File

    def test_resolve_whitespace_string_returns_file(self):
        """Whitespace-only string stripped becomes empty, resolves to current directory"""
        result = InputSourceResolver.resolve("   ")

        # "   ".strip() -> "" -> Path("") -> current directory -> exists -> File
        assert result == InputSource.File

    def test_resolve_json_string_returns_primitive(self):
        result = InputSourceResolver.resolve('{"key": "value"}')

        assert result == InputSource.Primitive

    def test_resolve_numeric_string_returns_primitive(self):
        result = InputSourceResolver.resolve("12345")

        assert result == InputSource.Primitive

    # --- Bytes Inputs ---

    def test_resolve_bytes_returns_primitive(self):
        result = InputSourceResolver.resolve(b"binary data")

        assert result == InputSource.Primitive

    def test_resolve_empty_bytes_returns_primitive(self):
        result = InputSourceResolver.resolve(b"")

        assert result == InputSource.Primitive

    # --- Numeric Inputs ---

    def test_resolve_integer_returns_primitive(self):
        result = InputSourceResolver.resolve(42)

        assert result == InputSource.Primitive

    def test_resolve_float_returns_primitive(self):
        result = InputSourceResolver.resolve(3.14)

        assert result == InputSource.Primitive

    # --- Boolean Inputs ---

    def test_resolve_boolean_true_returns_primitive(self):
        result = InputSourceResolver.resolve(True)

        assert result == InputSource.Primitive

    def test_resolve_boolean_false_returns_primitive(self):
        result = InputSourceResolver.resolve(False)

        assert result == InputSource.Primitive

    # --- List/Dict Inputs ---

    def test_resolve_list_returns_primitive(self):
        result = InputSourceResolver.resolve([1, 2, 3])

        assert result == InputSource.Primitive

    def test_resolve_dict_returns_primitive(self):
        result = InputSourceResolver.resolve({"key": "value"})

        assert result == InputSource.Primitive

    # --- Sync Stream Inputs ---

    def test_resolve_bytes_io_returns_stream(self):
        stream = BytesIO(b"test data")

        result = InputSourceResolver.resolve(stream)

        assert result == InputSource.Stream

    def test_resolve_string_io_returns_stream(self):
        stream = StringIO("test data")

        result = InputSourceResolver.resolve(stream)

        assert result == InputSource.Stream

    def test_resolve_file_handle_returns_stream(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        with open(test_file, "rb") as f:
            result = InputSourceResolver.resolve(f)

        assert result == InputSource.Stream

    def test_resolve_custom_sync_stream_returns_stream(self):
        class CustomSyncStream:
            def read(self):
                return b"data"

            def __iter__(self):
                return self

            def __next__(self):
                raise StopIteration

        stream = CustomSyncStream()
        result = InputSourceResolver.resolve(stream)

        assert result == InputSource.Stream

    def test_resolve_object_with_only_read_returns_primitive(self):
        """Object with read but not __iter__/__next__ is not a stream"""
        class NotAStream:
            def read(self):
                return b"data"

        obj = NotAStream()
        result = InputSourceResolver.resolve(obj)

        assert result == InputSource.Primitive

    # --- Async Stream Inputs ---

    def test_resolve_async_stream_returns_stream(self):
        class CustomAsyncStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def read(self):
                return b"data"

        stream = CustomAsyncStream()
        result = InputSourceResolver.resolve(stream)

        assert result == InputSource.Stream

    def test_resolve_object_with_aiter_but_no_read_returns_primitive(self):
        """Object with __aiter__/__anext__ but no async read is not a stream"""
        class NotAnAsyncStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

        obj = NotAnAsyncStream()
        result = InputSourceResolver.resolve(obj)

        assert result == InputSource.Primitive

    def test_resolve_object_with_sync_read_not_async_returns_primitive(self):
        """Object with __aiter__/__anext__ but sync read is not an async stream"""
        class NotAnAsyncStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            def read(self):  # Sync read, not async
                return b"data"

        obj = NotAnAsyncStream()
        result = InputSourceResolver.resolve(obj)

        assert result == InputSource.Primitive


class TestInputSourceResolverIsUrl:
    """Tests for InputSourceResolver._is_url() method"""

    def test_is_url_http(self):
        assert InputSourceResolver._is_url("http://example.com") is True

    def test_is_url_https(self):
        assert InputSourceResolver._is_url("https://example.com") is True

    def test_is_url_with_path(self):
        assert InputSourceResolver._is_url("https://example.com/path/to/resource") is True

    def test_is_url_with_query(self):
        assert InputSourceResolver._is_url("https://example.com?query=param") is True

    def test_is_url_ftp_returns_false(self):
        assert InputSourceResolver._is_url("ftp://example.com") is False

    def test_is_url_file_scheme_returns_false(self):
        assert InputSourceResolver._is_url("file:///path/to/file") is False

    def test_is_url_no_scheme_returns_false(self):
        assert InputSourceResolver._is_url("example.com") is False

    def test_is_url_no_netloc_returns_false(self):
        assert InputSourceResolver._is_url("http:///path") is False

    def test_is_url_empty_string_returns_false(self):
        assert InputSourceResolver._is_url("") is False

    def test_is_url_plain_text_returns_false(self):
        assert InputSourceResolver._is_url("hello world") is False


class TestInputSourceResolverIsSyncStream:
    """Tests for InputSourceResolver._is_sync_stream() method"""

    def test_is_sync_stream_bytes_io(self):
        stream = BytesIO(b"test")
        assert InputSourceResolver._is_sync_stream(stream) is True

    def test_is_sync_stream_string_io(self):
        stream = StringIO("test")
        assert InputSourceResolver._is_sync_stream(stream) is True

    def test_is_sync_stream_custom_stream(self):
        class CustomStream:
            def read(self):
                return b""

            def __iter__(self):
                return self

            def __next__(self):
                raise StopIteration

        assert InputSourceResolver._is_sync_stream(CustomStream()) is True

    def test_is_sync_stream_missing_read_returns_false(self):
        class NotAStream:
            def __iter__(self):
                return self

            def __next__(self):
                raise StopIteration

        assert InputSourceResolver._is_sync_stream(NotAStream()) is False

    def test_is_sync_stream_missing_iter_returns_false(self):
        class NotAStream:
            def read(self):
                return b""

            def __next__(self):
                raise StopIteration

        assert InputSourceResolver._is_sync_stream(NotAStream()) is False

    def test_is_sync_stream_missing_next_returns_false(self):
        class NotAStream:
            def read(self):
                return b""

            def __iter__(self):
                return self

        assert InputSourceResolver._is_sync_stream(NotAStream()) is False

    def test_is_sync_stream_string_returns_false(self):
        assert InputSourceResolver._is_sync_stream("hello") is False

    def test_is_sync_stream_bytes_returns_false(self):
        assert InputSourceResolver._is_sync_stream(b"hello") is False

    def test_is_sync_stream_list_returns_false(self):
        # List has __iter__ and __next__ but no read
        assert InputSourceResolver._is_sync_stream([1, 2, 3]) is False


class TestInputSourceResolverIsAsyncStream:
    """Tests for InputSourceResolver._is_async_stream() method"""

    def test_is_async_stream_valid(self):
        class AsyncStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def read(self):
                return b""

        assert InputSourceResolver._is_async_stream(AsyncStream()) is True

    def test_is_async_stream_missing_aiter_returns_false(self):
        class NotAsyncStream:
            async def __anext__(self):
                raise StopAsyncIteration

            async def read(self):
                return b""

        assert InputSourceResolver._is_async_stream(NotAsyncStream()) is False

    def test_is_async_stream_missing_anext_returns_false(self):
        class NotAsyncStream:
            def __aiter__(self):
                return self

            async def read(self):
                return b""

        assert InputSourceResolver._is_async_stream(NotAsyncStream()) is False

    def test_is_async_stream_missing_read_returns_false(self):
        class NotAsyncStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

        assert InputSourceResolver._is_async_stream(NotAsyncStream()) is False

    def test_is_async_stream_sync_read_returns_false(self):
        class NotAsyncStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            def read(self):  # Sync, not async
                return b""

        assert InputSourceResolver._is_async_stream(NotAsyncStream()) is False

    def test_is_async_stream_sync_stream_returns_false(self):
        stream = BytesIO(b"test")
        assert InputSourceResolver._is_async_stream(stream) is False

    def test_is_async_stream_string_returns_false(self):
        assert InputSourceResolver._is_async_stream("hello") is False


class TestInputSourceResolverEdgeCases:
    """Edge case tests for InputSourceResolver"""

    def test_resolve_path_object_to_existing_file(self, tmp_path):
        """Path object (not string) to existing file"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        result = InputSourceResolver.resolve(Path(test_file))

        assert result == InputSource.File

    def test_resolve_relative_path_string_existing(self, tmp_path, monkeypatch):
        """Relative path string that exists"""
        monkeypatch.chdir(tmp_path)
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        result = InputSourceResolver.resolve("test.txt")

        assert result == InputSource.File

    def test_resolve_url_like_string_not_url(self):
        """String that looks like URL but isn't valid"""
        result = InputSourceResolver.resolve("http://")

        assert result == InputSource.Primitive

    def test_resolve_localhost_url(self):
        result = InputSourceResolver.resolve("http://localhost:8080/api")

        assert result == InputSource.Link

    def test_resolve_ip_address_url(self):
        result = InputSourceResolver.resolve("http://192.168.1.1/resource")

        assert result == InputSource.Link

    def test_resolve_complex_object_returns_primitive(self):
        class ComplexObject:
            def __init__(self):
                self.value = 42

        result = InputSourceResolver.resolve(ComplexObject())

        assert result == InputSource.Primitive

    def test_resolve_callable_returns_primitive(self):
        def my_function():
            pass

        result = InputSourceResolver.resolve(my_function)

        assert result == InputSource.Primitive

    def test_resolve_lambda_returns_primitive(self):
        result = InputSourceResolver.resolve(lambda x: x)

        assert result == InputSource.Primitive