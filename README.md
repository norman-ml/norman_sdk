# Norman SDK Overview

Welcome to the Norman SDK - the developer toolkit for interacting with the Norman AI platform.

Norman SDK offers a clean, intuitive interface for uploading and invoking AI models.

Designed for developers, researchers, and production pipelines,
the Norman SDK abstracts away the complexity of running AI models
and gives you a powerful API for all model operations.

Main capabilities:
- Easy model upload workflows.
- Simple invocation of deployed models.
- API key registration management.

For the full reference and detailed instructions, please visit our SDK documentation at https://sdk.norman-ai.com.

Take your first steps with the Norman API.


## 1. Install the Norman SDK

To use the Norman API in Python, install the official package using pip:

```bash
pip install norman
```


## 2. Signup and create an API key

Before making any requests, you’ll need to create an **API key**.  
This key authorizes your SDK to securely access the Norman API.

```python
from norman_objects.services.authenticate.signup.signup_key_response import SignupKeyResponse

from norman import Norman

# Signup up to Norman
signup_response: SignupKeyResponse = await Norman.signup("<username>")

# Retrieve the API key from the signup response 
api_key: str = signup_response.api_key  
```

⚠️ **Important:**  
> Store your API key securely. API keys **cannot be regenerated** - if you lose your key, you will lose access to all your models and data across Norman clients.

## 3. Run your first model
With the Norman SDK, running a model is straightforward. You select a model from our [Model Library](https://norman-ai.com/library), check the required inputs and their format, and invoke the model using a simple API call.

Norman makes a distinction between deploying a model and invoking it. We call their configuration classes, respectively, the Model configuration and the Invocation configuration.



### Define an invocation configuration
Model owners define what is called the model signature when deploying to Norman. A model signature consists of input and output signatures, which collectively define how Norman should transform user input so the model can run it and vice versa.

To run a model we must first define an invocation configuration object, defining how our input data maps to the model input signature:

In this example we will define a configuration object for an image generation model called Stable Diffusion 3.5 Large. This particular model expects one text input from the user to be mapped to the "Prompt" parameter in the model:

```python
from typing import Any

# Define an invocation configuration dictionary
invocation_config: dict[str, Any] = {
    "model_name": "stable-diffusion-3.5-large",
    "inputs": [
        {
            "display_title": "Prompt",
            "data": "A cat playing with a ball on Mars"
        }
    ]
}
```

### Invoke the model
Running a model is an asynchronous process that can take several minutes to complete, the duration depends on the model size, the input size and the load on the Norman service.  

For more granular control and advanced configuration options, please have a look at the [Norman Core SDK documentation](https://sdk.norman-ai.com/api/core/overviewcore/overviewcore).
```python
from norman import Norman

# Initialize a Norman client
norman = Norman(api_key="<your_api_key>")

# Invoke the model
invocation_response: dict[str, bytes] = await norman.invoke(invocation_config)
```


### Use the model outputs
Each model uploaded to Norman has a unique output signature. When you invoke a model, the response is returned according to the structure of the output signature, formatted as a dictionary.

- Dictionary keys each map to an output display title.
- Values are binary byte streams encoding the model output.

Output values can be consumed and used in a variety of ways, according to the needs of each user. Stable Diffusion 3.5 Large exposes one output parameter called "Image" which users can consume:

```python
from io import BytesIO

from PIL import Image

# Get the raw image bytes from the invocation response
image_bytes: bytes = invocation_response["Image"]
byte_stream: BytesIO = BytesIO(image_bytes)

# Load the image from the response byte stream
image: Image = Image.open(byte_stream)
image.show(title="stable-diffusion-3.5-large output image")

# Optionally save the image to disk
image.save("stable_diffusion_3_5_large_output_image.png")
```


## 4. Optional - deploy your own model

Norman provides a large selection of ready to run AI models. If you want to deploy your own model, you can provide the model files, define the input and output signatures and configure the user interface. 

You can use the SDK to deploy your model to the Norman servers, letting you run it just as you would run any other model in the model library.


Below is a complete example showing how to upload the same stable-diffusion-3.5-large model as before. We want to map the true input and output signatures of the model, as defined by the model creators, into user friendly display titles that can be intuitively used during invocation.

For the full specification, please visit [Norman SDK documentation](https://sdk.norman-ai.com/api/norman1/overview1#3)

### Define a model configuration
The model configuration defines how Norman deploys your model and what interfaces it should expose for end user invocation. It describes the model properties, assets and signature alongside many optional properties such as tags and parameters.

it includes:

- Basic information such as the model name, version label, and descriptions that appear in the model library.

- Assets configurations such as the model logo and weight files, associated with the model or required for inference.

- Input and output signatures as a structured mapping between user data and model parameters, along with all supporting parameters required to perform the conversions and display the interface for the model in the Norman library.

In this example we will define a configuration object for the model that displays a text input widget to receive user input, and an image output widget to render the model output:
```python
from typing import Any


# Define a model configuration dictionary
model_config: dict[str, Any] = {
    "name":"stable-diffusion-3.5-large",
    "category":"diffusion",
    "version":{
        "label":"beta",
        "short_description": "A text-to-image diffusion model for high-quality image generation.",
        "long_description": ("""
            Stable Diffusion 3.5 large is a latent text-to-image diffusion model trained on
            a large-scale dataset to generate detailed and diverse images from natural
            language prompts. It serves as a general-purpose foundation model that can
            be fine-tuned for tasks such as image generation, editing, and style transfer.
        """),    
        "assets":[
            {
                "asset_name": "File",
                "data": "<path/to/your/file>"
            },
            {
                "asset_name": "Logo",
                "data": "<path/to/your/file>"
            }
        ],
        "inputs":[
            {
                "display_title": "Prompt",
                "container_modality": "Text",
                "data_domain": "prompt",
                "receive_format": "primitive",
                "parameters": [
                    {
                        "name":"prompt",
                        "channel_modality": "Text"
                    }
                ]
            }
        ],
        "outputs":[
            {
                "display_title": "Image",
                "container_modality": "Image",
                "data_domain": "generate_image",
                "receive_format": "File",
                "parameters": [
                    {
                        "name":"images[0]",
                        "channel_modality": "Image"
                    }
                ]
            }
        ]
    }
}
```

### Upload the model
Once the model configuration is defined, all you need to deploy it is to perform a single API call. Norman uploads your configuration, stores the model assets, registers the input and output signatures, creates a new listing in the model library and makes the model available for invocation.

```python
from norman_objects.shared.models.model_projection import ModelProjection

from norman import Norman

# Initialize a Norman client
norman = Norman(api_key="<your_api_key>")

# Upload your model to Norman
model: ModelProjection = await norman.upload_model(model_config)
```

## What happens next?

Once the model has been deployed, you can run your model as many times as you need. Invocation is done using the same code used in the [Run your first model](https://github.com/norman-ml/norman_sdk_python/?tab=readme-ov-file#3-run-your-first-model) section.
