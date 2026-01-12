import asyncio
import os
import tempfile
from norman import Norman

# --- CONFIGURATION ---
API_KEY = "9iHXIPzQ1rDdAxpIlj5PmOJTIkIZAnmDk143fn14GQWI1dnB347WC7xKp57IkogLhCzlrMpUFJEN7Wz5TrK_3MCQtpE_gThghlD2ovOIZN1TQGt_"
MODEL_NAME = "IntegTest_b9239b50"

# MUST match SignatureConfig.display_title EXACTLY
INPUT_TITLE = "Original image"
OUTPUT_TITLE = "Mirrored image"

# Path to your input image
INPUT_IMAGE_PATH = r"D:\dev\norman\norman_sdk_python\tests\assets\sample_inputs\sample_input.png"


async def run_invocation():
    print(f"--- Starting Invocation for {MODEL_NAME} ---")

    if not os.path.exists(INPUT_IMAGE_PATH):
        print(f"ERROR: Input image not found at {INPUT_IMAGE_PATH}")
        return

    norman = Norman(api_key=API_KEY)

    try:
        invocation_config = {
            "model_name": MODEL_NAME,
            "inputs": [
                {
                    "display_title": INPUT_TITLE,
                    "data": INPUT_IMAGE_PATH,
                    "source": "File"
                }
            ]
        }

        print("Invoking image model...")

        result = await norman.invoke(invocation_config)

        if OUTPUT_TITLE not in result:
            print(f"\nERROR: Output title '{OUTPUT_TITLE}' not found.")
            print(f"Available outputs: {list(result.keys())}")
            return

        output_bytes = result[OUTPUT_TITLE]

        # Save mirrored image to disk
        output_path = os.path.join(
            tempfile.gettempdir(),
            f"mirrored_output_{MODEL_NAME}.png"
        )

        with open(output_path, "wb") as f:
            f.write(output_bytes)

        print("\n" + "=" * 30)
        print("INVOCATION SUCCESS!")
        print(f"Input image:   {INPUT_IMAGE_PATH}")
        print(f"Output image:  {output_path}")
        print("=" * 30)

    except Exception as e:
        print("\n--- INVOCATION FAILED ---")
        print(f"Error Detail: {e}")


if __name__ == "__main__":
    asyncio.run(run_invocation())
