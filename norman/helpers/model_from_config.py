from typing import Any

from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.model_signature import ModelSignature
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from norman_objects.shared.model_signatures.signature_type import SignatureType
from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model import Model
from norman_objects.shared.models.model_asset import ModelAsset
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.output_format import OutputFormat
from norman_objects.shared.parameters.data_domain import DataDomain
from norman_objects.shared.parameters.model_param import ModelParam


class ModelFromConfig:
    @staticmethod
    def create_model(account_id: str, config: dict[str, Any]) -> Model:
        base_config: dict[str, Any] = {
            "version_label": "v1.0",
            "hosting_location": "Internal",
            "output_format": "Json",
            "request_type": "Post",
            "http_headers": {},
            "url": ""
        }

        base_config.update(config)
        if base_config["hosting_location"] == "Internal":
            base_config["model_type"] = "Pytorch_jit"
        else:
            base_config["model_type"] = "Api"

        ModelFromConfig._validate_fields(base_config)

        model = Model(
            name = base_config["name"],
            account_id = account_id,
            version_label = base_config["version_label"],
            short_description = base_config["short_description"],
            url = base_config["url"],
            request_type = HttpRequestType(base_config["request_type"]),
            model_type = ModelType(base_config["model_type"]),
            hosting_location = ModelHostingLocation(base_config["hosting_location"]),
            output_format = OutputFormat(base_config["output_format"]),
            long_description = base_config["long_description"],
            http_headers = base_config["http_headers"],
        )

        model.inputs = ModelFromConfig._create_model_signatures(SignatureType.Input, base_config["inputs"])
        model.outputs = ModelFromConfig._create_model_signatures(SignatureType.Output, base_config["outputs"])
        model.assets = ModelFromConfig._create_model_assets(account_id, base_config["assets"])

        return model

    @staticmethod
    def _create_model_signatures(signature_type: SignatureType, signature_configs: list[dict[str, Any]]) -> list[ModelSignature]:
        basic_config: dict[str, Any] = {
            "receive_format": "File",
            "http_location": "Body",
            "default_value": None
        }
        signatures = []
        for signature_config in signature_configs:
            config = basic_config.copy()
            config.update(signature_config)

            ModelFromConfig._validate_signature_fields(config)

            model_signature = ModelSignature(
                display_title = config["display_title"],
                signature_type = signature_type,
                data_domain = DataDomain(config["data_domain"]),
                data_encoding = config["data_encoding"],
                receive_format = ReceiveFormat(config["receive_format"]),
                http_location = HttpLocation(config["http_location"]),
                hidden = False,
                default_value = config["default_value"],
            )

            for param in config["parameters"]:
                model_param = ModelParam(
                    parameter_name=param["parameter_name"],
                    data_domain=param["data_domain"],
                    data_encoding=param["data_encoding"]
                )
                model_signature.parameters.append(model_param)


            signatures.append(model_signature)

        return signatures

    @staticmethod
    def _create_model_assets(account_id: str, assets: list[dict[str, Any]]) -> list[ModelAsset]:
        model_assets: list[ModelAsset] = []
        for asset in assets:
            model_asset = ModelAsset(
                account_id = account_id,
                asset_name=asset["asset_name"],
            )
            model_assets.append(model_asset)

        return model_assets

    @staticmethod
    def _validate_signature_fields(config: dict[str, Any]):
        required_fields = ["display_title", "data_domain", "data_encoding", "parameters"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"{field} must be provided")

        if len(config["parameters"]) == 0:
            raise ValueError("parameters must be provided")

        required_param_fields = ["parameter_name", "data_domain", "data_encoding"]
        for param in config["parameters"]:
            for field in required_param_fields:
                if field not in param:
                    raise ValueError(f"{field} must be provided for each parameter")

    @staticmethod
    def _validate_fields(config: dict[str, Any]):
        required_fields = ["name", "short_description", "long_description", "inputs", "outputs", "assets"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"{field} must be provided")

        if config["hosting_location"] == "External":
            if "url" not in config or config["url"] == "":
                raise ValueError("url must be provided when hosting_location is external")
            if "http_headers" not in config:
                raise ValueError("http_headers must be provided when request_type is Post")
