import logging
import os
from typing import Dict, Optional, Union

import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

from block_cascade.executors.databricks.resource import DatabricksResource
from block_cascade.executors.vertex.resource import GcpResource

logger = logging.getLogger(__name__)


GCP_RESOURCE = GcpResource.__name__
DATABRICKS_RESOURCE = DatabricksResource.__name__
SUPPORTED_FILENAMES = ("cascade.yaml", "cascade.yml")
ACCEPTED_TYPES = (GCP_RESOURCE, DATABRICKS_RESOURCE)


def _merge(a: dict, b: dict) -> dict:
    """
    Deep merges two dictionaries with values in b
    overriding values in a (if present).

    e.g. _merge({"hello": "world}, {"hello", "goodbye"}) -> {"hello": "goodbye"}

    This function is focused primarily at merging the values of nested dictionaries
    with all other types being simply overriden if keys are found in both input
    dictionaries.
    """
    merged = {}

    for key, val in a.items():
        if key not in b:
            merged[key] = val

    for key, val in b.items():
        if key not in a:
            merged[key] = val
        else:
            val_2 = a[key]
            if isinstance(val, dict) and isinstance(val_2, dict):
                merged[key] = _merge(val_2, val)
            else:
                merged[key] = val
    return merged


def find_default_configuration(
    root: str = ".",
) -> Optional[Dict[str, Union[GcpResource, DatabricksResource]]]:
    for filename in SUPPORTED_FILENAMES:
        potential_configuration_path = os.path.join(root, filename)
        if not os.path.exists(potential_configuration_path):
            continue

        logger.info(f"Found cascade configuration at {potential_configuration_path}")
        with open(potential_configuration_path) as f:
            configuration = yaml.load(f, SafeLoader)

        job_configurations = {}
        for job_name, resource_definition in configuration.items():
            if job_name == "default":
                continue
            default_resource_definition = configuration.get("default", {})
            resource_type = resource_definition.pop("type")
            if resource_type not in ACCEPTED_TYPES:
                raise ValueError(
                    f"Only types: {','.join(ACCEPTED_TYPES)} are supported for resource definitions."  # noqa: E501
                )
            elif resource_type == GCP_RESOURCE:
                merged_resource_definition = _merge(
                    default_resource_definition.get(GCP_RESOURCE, {}),
                    resource_definition,
                )
                job_configurations[job_name] = GcpResource(**merged_resource_definition)
            else:
                merged_resource_definition = _merge(
                    default_resource_definition.get(DATABRICKS_RESOURCE, {}),
                    resource_definition,
                )
                job_configurations[job_name] = DatabricksResource(
                    **merged_resource_definition
                )
        return job_configurations
    return None
