import pytest

from norman import Norman
from norman.objects.configs.invocation.invocation_config import InvocationConfig
from norman.objects.configs.invocation.invocation_input_config import InvocationInputConfig


@pytest.mark.usefixtures("api_key")
class TestModelInvoke:
    @pytest.mark.invocations
    async def test_invoke_model(self, api_key: str) -> None:
        request_text = """
            Tell Ea-nasir: Nanni sends the following message:  
            When you came, you said to me as follows: 
            "I will give Gimil-Sin (when he comes) fine quality copper ingots."
        """

        invocation_input_config = InvocationInputConfig(
            display_title="Original text",
            data=request_text
        )

        invocation_config = InvocationConfig(
            model_name="VinciText50 SDK",
            inputs=[invocation_input_config]
        )

        raw_invocation_config = invocation_config.model_dump()

        norman = Norman(api_key)
        invocation_response = await norman.invoke(raw_invocation_config)

        text_bytes = invocation_response["Reversed text"]
        response_text = text_bytes.decode("utf8")

        # We are testing a text reversal qa model
        assert response_text == request_text[::-1]
