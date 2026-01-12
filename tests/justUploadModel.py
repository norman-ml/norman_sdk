import asyncio
import secrets
import os

from norman import Norman
from norman.managers.authentication_manager import AuthenticationManager
from norman_utils_external.name_utils import NameUtils

from norman.objects.configs.model.model_projection_config import ModelProjectionConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig
from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.signature_config import SignatureConfig
from norman.objects.configs.model.parameter_config import ParameterConfig
from norman_objects.shared.parameters.data_modality import DataModality
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat


async def debug_upload_with_new_account():
    print("--- Starting Dynamic Account & Upload Process ---")

    # 1. Generate a new account and API Key
    try:
        account_name = NameUtils.generate_account_name()
        print(f"Creating new account: {account_name}...")

        signup_response = await AuthenticationManager.signup_and_generate_key(account_name)
        new_api_key = signup_response.api_key

        print(f"NEW API KEY GENERATED: {new_api_key}")
        print("-" * 50)
    except Exception as e:
        print(f"FAILED to create account: {e}")
        return

    # 2. Initialize Norman with the fresh key
    norman = Norman(api_key=new_api_key)

    # 3. Create a unique model name
    random_id = secrets.token_hex(4)
    model_name = f"IntegTest_{random_id}"

    # Path to your actual model file
    asset_path = r"D:\dev\norman\norman_sdk_python\tests\assets\model_files\text_qa_model.pt"

    if not os.path.exists(asset_path):
        print(f"ERROR: File not found at {asset_path}")
        return

    # 4. Build the configuration
    version_cfg = ModelVersionConfig(
        label="1.0.0",
        short_description="Diagnostic test model.",
        long_description="This is a long description required for valid registry entries.",
        assets=[AssetConfig(asset_name="File", data=asset_path)],
        inputs=[SignatureConfig(
            display_title="Original image",
            data_modality=DataModality.Image,
            data_domain="prompt",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            parameters=[ParameterConfig(parameter_name="raw_text", data_encoding="utf8")]
        )],
        outputs=[SignatureConfig(
            display_title="Mirrored image",
            data_modality=DataModality.Image,
            data_domain="llm",
            data_encoding="utf8",
            receive_format=ReceiveFormat.Primitive,
            parameters=[ParameterConfig(parameter_name="reverse_text", data_encoding="utf8")]
        )]
    )

    config = ModelProjectionConfig(
        name=model_name,
        category="Debug",
        version=version_cfg
    ).model_dump()

    print(f"Attempting to upload model: {model_name}...")

    # 5. Execute Upload
    try:
        model = await norman.upload_model(config)
        print("\n" + "=" * 30)
        print("UPLOAD SUCCESSFUL!")
        print(f"API KEY FOR INVOCATION: {new_api_key}")
        print(f"MODEL NAME:             {model.name}")
        print(f"MODEL ID:               {model.id}")
        print("=" * 30)
        print("\nCopy the API KEY and MODEL NAME above to use in your invocation script.")

    except Exception as e:
        print("\n--- UPLOAD FAILED ---")
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(debug_upload_with_new_account())