from typing import TypedDict, Any, Literal

InputSource = Literal["File", "Primitive", "Link", "Path"]

class ModelInput(TypedDict):
    source: InputSource
    data: Any

class InvocationConfig(TypedDict):
    model_name: str
    inputs: dict[str, ModelInput]
