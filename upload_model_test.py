import asyncio

import aiofiles

from norman import Norman
from norman.config_builders.model_builder import ModelBuilder
from norman.config_builders.model_signature_builder import ModelSignatureBuilder

link = "https://encrypted-tbn2.gstatic.com/images?q=tbn:ANd9GcQV-ielrEM4nyyG6fJhp0YThlkfFxa6WKe2qPWpRCWwpZtOjfC6Nh5NBgWiZlkSN1SDIw5b"
file_path = "D:/Norman/norman_core_sdk/tests/samples/sample_inputs/sample_input.jpg"
API_KEY = "2m8OcvniQDgoBUrOjcAxNfzcFHWkLwaj9p5Faf1wizE="
account_id = "23845703660070374099865734459398242515"

model_config = {
    "name": "my_test_model",
    "short_description": "This is a test model.",
    "long_description": "This is a test model. but with a longer description.",
     "assets": [
         {
             "asset_name": 'Logo',
             "source": "link",
             "data": link
         },
         {
             "asset_name": 'File',
             "source": "file",
             "data": "path/to/file.pt"
         },
    ],
    "inputs": [
        {
            'display_title': 'Input',
            'data_domain': 'Image',
            'data_encoding': 'png',
            'parameters': [
                {
                    'parameter_name': 'image',
                    'data_domain': 'Image',
                    'data_encoding': 'png',
                }
            ]
        }
    ],
    "outputs": [
        {
            'display_title': 'Input',
            'data_domain': 'Image',
            'data_encoding': 'png',
            'parameters': [
                {
                    'parameter_name': 'image',
                    'data_domain': 'Image',
                    'data_encoding': 'png',
                }
            ]
        }
    ]
}

model_config_from_builder = (
    ModelBuilder(
        "my_img_model",
        "This is a test model.",
        "This is a test model. but with a longer dd description."
    )
    .add_asset("Logo", "Path", "/Users/dolevgabay/Desktop/norman/sdk/img_input.jpg")
    .add_asset("File", "Path", "/Users/dolevgabay/Desktop/norman/sdk/image_qa_model.pt")
    .add_input(
        ModelSignatureBuilder("Straight Image", "Image", "png").add_parameter("image", "Image", "png").build()
    )
    .add_output(
        ModelSignatureBuilder("Mirrored Image", "Image", "png").add_parameter("mirror_image", "Image", "png").build()
    )
    .add_version_label("one")
).build()

async def main():
    print("Playground")
    norman = Norman(
        account_id="23846230582255309714505001774417502931",
        password="Dolevg1234!"
    )
    await norman.upload_model(model_config_from_builder)


if __name__ == "__main__":
    asyncio.run(main())