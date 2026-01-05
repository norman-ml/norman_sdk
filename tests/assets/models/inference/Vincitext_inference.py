from typing import Any

from diffusers import DiffusionPipeline
import torch

def load_model(model_path: str) -> DiffusionPipeline:
    pipeline = DiffusionPipeline.from_pretrained(model_path)
    return pipeline

def forward(pipeline: DiffusionPipeline, model_inputs: dict[str, Any]) -> dict[str, Any]:
    model_inputs["generator"] = torch.Generator(device="cuda")

    with torch.inference_mode():
        model_outputs: dict[str, Any] = pipeline(**model_inputs)

    del model_inputs["generator"]

    return model_outputs

def clear(pipeline: DiffusionPipeline):
    pipeline.to("cpu")
    del pipeline

    torch.cuda.synchronize()
    torch.cuda.empty_cache()