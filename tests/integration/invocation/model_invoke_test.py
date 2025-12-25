import asyncio
import os
import tempfile
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio

from norman import Norman
from norman_objects.shared.models.model_projection import ModelProjection


@dataclass
class InvocationTestData:
    result: dict[str, Any]
    config: dict
    norman: Norman
    model: ModelProjection
    input_display_title: str
    output_display_title: str
    original_input_text: str

@pytest_asyncio.fixture(scope="class")
async def invocation_result(uploaded_model) -> InvocationTestData:
    model = uploaded_model.model
    norman = uploaded_model.norman
    model_config = uploaded_model.config
    entity_ids = uploaded_model.entity_ids

    input_display_title = model_config["version"]["inputs"][0]["display_title"]
    output_display_title = model_config["version"]["outputs"][0]["display_title"]

    print(f"\n[Fixture] Waiting for model '{model.name}' to be provisioned...")

    max_retries = 60
    is_ready = False

    token = norman._authentication_manager.access_token

    for i in range(max_retries):
        from norman_objects.shared.queries.query_constraints import QueryConstraints
        from norman_objects.shared.status_flags.status_flag_value import StatusFlagValue

        constraints = QueryConstraints.includes("Status_Flags", "Entity_ID", entity_ids)

        try:
            flags_dict = await norman._invocation_manager._persist_service.status_flags.get_status_flags(
                token,
                constraints.model_dump(mode="json")
            )

            all_entities_finished = True
            for eid in entity_ids:
                flags = flags_dict.get(eid, [])
                if not any(f.flag_value == StatusFlagValue.Finished for f in flags):
                    all_entities_finished = False
                    break

            if all_entities_finished:
                print(f"[Fixture] All entities are READY.")
                is_ready = True
                break
        except Exception as e:
            print(f"[Fixture] Warning: Status check failed: {e}. Retrying...")

        print(f"[Fixture] Still provisioning... (Attempt {i + 1}/{max_retries})")
        await asyncio.sleep(5)

    if not is_ready:
        pytest.fail(f"Model {model.name} failed to reach 'Finished' state within 5 minutes.")

    # 3. Create a temporary file and Invoke
    print(f"[Fixture] Invoking model {model.name}...")
    test_input = "Hello, world! This is a test invocation."

    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
    temp_path = temp_file.name
    try:
        temp_file.write(test_input)
        temp_file.close()

        invocation_config = {
            "model_name": model.name,
            "inputs": [
                {
                    "display_title": input_display_title,
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
            model=model,
            input_display_title=input_display_title,
            output_display_title=output_display_title,
            original_input_text=test_input
        )
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)

@pytest.mark.timeout(480)
class TestModelInvoke:
    @pytest.mark.invocations
    async def test_create_invocation(self, invocation_result: InvocationTestData):
        result = invocation_result.result
        output_title = invocation_result.output_display_title
        assert result is not None
        assert output_title in result, f"Key '{output_title}' missing. Got: {list(result.keys())}"

    @pytest.mark.invocations
    async def test_invocation_output_format(self, invocation_result: InvocationTestData):
        result = invocation_result.result
        output = result[invocation_result.output_display_title]
        assert isinstance(output, (bytes, bytearray))
        assert len(output) > 0
        # Check UTF-8 validity
        assert output.decode('utf-8')

    @pytest.mark.invocations
    async def test_invocation_processes_input(self, invocation_result: InvocationTestData):
        actual_text = invocation_result.result[invocation_result.output_display_title].decode('utf-8')
        # Helper: Reverse the actual input string we sent
        expected_text = invocation_result.original_input_text[::-1]

        # Using strip() to be lenient with trailing newlines from the server
        assert actual_text.strip() == expected_text.strip(), (
            f"Logic error!\nExpected: {expected_text}\nGot: {actual_text}"
        )
