import os
import tempfile
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio

from norman import Norman


# API key for the account that owns NogaModel18
NOGA_API_KEY = "8tQzjBJ6YUDH0VWS-5TysffwT_rdZXmLSLbrw3uUl_ZFCddEiDa9lOP6eJb7nXfutmZHA1dBYR7EJu7miVdm-BZJrdWLLtEyaN7NmR7q9NwK21s3"

# Model configuration for NogaModel18
NOGA_MODEL_NAME = "NogaModel18"
NOGA_INPUT_DISPLAY_TITLE = "test"
NOGA_OUTPUT_DISPLAY_TITLE = "xd"


@dataclass
class InvocationTestData:
    """Container bundling invocation test data for reuse across tests."""
    result: dict[str, Any]  # Output from invoke()
    config: dict  # Invocation config used
    norman: Norman  # Client instance
    model_name: str  # Name of the model that was invoked


@pytest_asyncio.fixture(scope="class")
async def invocation_result() -> InvocationTestData:
    """
    Invoke the existing NogaModel18 once for all invocation tests in this class.

    Uses a pre-existing model to avoid the ~10 minute model upload time.

    Yields:
        InvocationTestData with invocation results and metadata
    """
    print(f"\n[Fixture] Invoking model {NOGA_MODEL_NAME}...")

    norman = Norman(api_key=NOGA_API_KEY)

    # Create a temporary file for the input data
    # (Workaround for SDK bug in file_utils.get_buffer_size with BytesIO)
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
    temp_path = temp_file.name
    try:
        temp_file.write("Hello, world! This is a test invocation.")
        temp_file.close()

        invocation_config = {
            "model_name": NOGA_MODEL_NAME,
            "inputs": [
                {
                    "display_title": NOGA_INPUT_DISPLAY_TITLE,
                    "data": temp_path,
                    "source": "File"
                }
            ]
        }

        result = await norman.invoke(invocation_config)
        print(f"[Fixture] Invocation complete")

        yield InvocationTestData(
            result=result,
            config=invocation_config,
            norman=norman,
            model_name=NOGA_MODEL_NAME
        )
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)


class TestModelInvoke:
    """
    Test suite for model invocation functionality.

    All tests share a single invocation via the class-scoped invocation_result fixture.
    Uses existing model NogaModel18 to avoid model upload time.
    """

    @pytest.mark.invocations
    async def test_create_invocation(self, invocation_result: InvocationTestData):
        """
        Verify invocation was created and executed successfully.

        Checks:
        - Invocation returned a result (not None or empty)
        - Result contains expected output keys
        """
        result = invocation_result.result

        # Verify result exists
        assert result is not None, "Invocation should return a result"
        assert len(result) > 0, "Invocation result should not be empty"

        # Verify expected output key exists
        assert NOGA_OUTPUT_DISPLAY_TITLE in result, (
            f"Result should contain output '{NOGA_OUTPUT_DISPLAY_TITLE}', got keys: {list(result.keys())}"
        )

    @pytest.mark.invocations
    async def test_invocation_output_format(self, invocation_result: InvocationTestData):
        """
        Verify invocation output has correct format and content.

        Checks:
        - Output is a bytearray
        - Output contains data (not empty)
        - Output can be decoded as text
        """
        result = invocation_result.result
        output = result[NOGA_OUTPUT_DISPLAY_TITLE]

        # Verify output type
        assert isinstance(output, (bytes, bytearray)), (
            f"Output should be bytes or bytearray, got {type(output)}"
        )

        # Verify output has content
        assert len(output) > 0, "Output should not be empty"

        # Verify output can be decoded as text (since it's a text model)
        try:
            decoded = output.decode('utf-8')
            assert len(decoded) > 0, "Decoded output should not be empty"
        except UnicodeDecodeError:
            pytest.fail("Output should be valid UTF-8 text")

    @pytest.mark.invocations
    async def test_invocation_processes_input(self, invocation_result: InvocationTestData):
        """
        Verify the model actually processed the input.

        The test model reverses text, so we verify the output
        is the reversed version of our input.
        """
        result = invocation_result.result
        output = result[NOGA_OUTPUT_DISPLAY_TITLE]

        # Decode output
        output_text = output.decode('utf-8')
        print(output_text)
        # Our input was "Hello, world! This is a test invocation."
        # The model reverses it
        expected_output = ".noitacovni tset a si sihT !dlrow ,olleH"

        assert output_text == expected_output, (
            f"Model should reverse the input text.\n"
            f"Expected: {expected_output}\n"
            f"Got: {output_text}"
        )