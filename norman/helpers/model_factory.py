from typing import Any

from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.models.model import Model
from norman_objects.shared.models.model_asset import ModelAsset


class ModelFactory:
    @staticmethod
    def create_model(account_id: str, config: dict[str, Any]) -> Model:
        inputs = config.pop("inputs", [])
        outputs = config.pop("outputs", [])
        assets = config.pop("assets", [])

        model_inputs = [ModelSignature(signature_type=SignatureType.Input, **input) for input in inputs]
        model_outputs = [ModelSignature(signature_type=SignatureType.Output, **output) for output in outputs]
        model_assets = [ModelAsset(account_id=account_id, **asset) for asset in assets]

        model = Model(
            account_id=account_id,
            inputs=model_inputs,
            outputs=model_outputs,
            assets=model_assets,
            **config
        )

        config["inputs"] = inputs
        config["outputs"] = outputs
        config["assets"] = assets

        return model
