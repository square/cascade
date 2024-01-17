import click
import json
from google.cloud import aiplatform_v1beta1 as aiplatform
from google.protobuf import json_format

from block_cascade import GcpResource
from block_cascade.config import find_default_configuration
from block_cascade.consts import SERVICE
from block_cascade.utils import get_gcloud_config


def get_gcp_project(config: dict) -> str:
    """Return the GCP project from a local config."""
    return config.get("core", {})["project"]


def get_gcp_region(config: dict) -> str:
    """Return the GCP region from a local config."""
    return config.get("compute", {})["region"]


def get_persistent_resource_payload(resource: GcpResource) -> dict:
    """Generate a persistent resource payload from a GcpResource."""

    CPU_MACHINE_TYPE = resource.chief.type
    CPU_REPLICA_COUNT = resource.chief.count
    CPU_MIN_REPLICA_COUNT = resource.chief.min_replica_count or 0
    CPU_MAX_REPLICA_COUNT = resource.chief.max_replica_count or 0
    if resource.chief.accelerator is not None:
        GPU_MACHINE_TYPE = resource.chief.type
        GPU_REPLICA_COUNT = resource.chief.count
        GPU_ACCELERATOR_TYPE = resource.chief.accelerator.type
        GPU_ACCELERATOR_COUNT = resource.chief.accelerator.count
    else:
        GPU_MACHINE_TYPE = None
        GPU_REPLICA_COUNT = 0
        GPU_ACCELERATOR_TYPE = None
        GPU_ACCELERATOR_COUNT = 0

    # This is consistent with the default disk spec of jobs.
    DISK_SPEC = {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100}

    CPU_AUTOSCALING_SPEC = {
        "min_replica_count": CPU_MIN_REPLICA_COUNT,
        "max_replica_count": CPU_MAX_REPLICA_COUNT,
    }

    CPU_POOL = {
        "machine_spec": {
            "machine_type": CPU_MACHINE_TYPE,
        },
        "replica_count": CPU_REPLICA_COUNT,
        "disk_spec": DISK_SPEC,
    }
    if CPU_MIN_REPLICA_COUNT > 0 or CPU_MAX_REPLICA_COUNT > 0:
        CPU_POOL["autoscaling_spec"] = CPU_AUTOSCALING_SPEC

    GPU_POOL = {
        "machine_spec": {
            "machine_type": GPU_MACHINE_TYPE,
            "accelerator_type": GPU_ACCELERATOR_TYPE,
            "accelerator_count": GPU_ACCELERATOR_COUNT,
        },
        "replica_count": GPU_REPLICA_COUNT,
        "disk_spec": DISK_SPEC,
    }

    if CPU_REPLICA_COUNT > 0 and GPU_REPLICA_COUNT > 0:
        RESOURCE_POOLS = [CPU_POOL, GPU_POOL]
    elif CPU_REPLICA_COUNT > 0:
        RESOURCE_POOLS = [CPU_POOL]
    elif GPU_REPLICA_COUNT > 0:
        RESOURCE_POOLS = [GPU_POOL]
    else:
        RESOURCE_POOLS = []

    PERSISTENT_RESOURCE = {
        "display_name": "Preview Persistent Resource",
        "resource_pools": RESOURCE_POOLS,  # You may remove below pools or add more pools if you want.
    }

    return PERSISTENT_RESOURCE


def get_parent_str(resource: GcpResource) -> str:
    """Return the parent string for the given project and region."""
    project_id = resource.environment.project
    region = resource.environment.region
    return f"projects/{project_id}/locations/{region}"


def get_endpoint_str(region: str) -> str:
    """Return the parent string for the given project and region."""
    return f"{region}-{SERVICE}"


@click.command()
@click.option(
    "--name",
    "-n",
    required=True,
    help="Name for the persistent resource.",
)
def create_persistent_resource(name: str):
    resource = find_default_configuration()[name]
    persistent_resource = get_persistent_resource_payload(resource)

    # CreatePersistentResoure SDK
    region = resource.environment.region
    client_options = {"api_endpoint": f"{region}-{SERVICE}"}
    client = aiplatform.PersistentResourceServiceClient(client_options=client_options)

    parent = get_parent_str(resource)

    try:
        operation = client.create_persistent_resource(
            parent=parent,
            persistent_resource_id=name,
            persistent_resource=persistent_resource,
        )
        response = operation.result()
        response = json.loads(json_format.MessageToJson(response._pb))
    except Exception as e:
        print(e)
        response = {}
    return response


@click.command()
@click.option(
    "--project",
    "-p",
    help="GCP project ID. Inferred from gcloud config if not provided.",
)
@click.option(
    "--region",
    "-r",
    help="GCP region. Inferred from gcloud config if not provided.",
)
def list_persistent_resources(region: str = None, project: str = None):
    # try to infer project and region from gcloud config
    if region is None or project is None:
        gcp_config = get_gcloud_config()
        if project is None:
            project = get_gcp_project(gcp_config)
        if region is None:
            region = get_gcp_region(gcp_config)

    client_options = {"api_endpoint": f"{region}-{SERVICE}"}
    client = aiplatform.PersistentResourceServiceClient(client_options=client_options)

    request = aiplatform.ListPersistentResourcesRequest(
        parent=f"projects/{project}/locations/{region}"
    )

    response = client.list_persistent_resources(request=request)
    response_json = json.loads(json_format.MessageToJson(response._pb))
    print(
        "Listing Persistent Resources:\n",
        json.dumps(response_json, indent=2),
    )


@click.command()
@click.option(
    "--persistent-resource-id",
    "-i",
    required=True,
    help="ID of the persistent resource to delete.",
)
@click.option(
    "--project",
    "-p",
    help="GCP project ID. Inferred from gcloud config if not provided.",
)
@click.option(
    "--region",
    "-r",
    help="GCP region. Inferred from gcloud config if not provided.",
)
def delete_persistent_resource(
    persistent_resource_id: str, region: str = None, project: str = None
):
    # try to infer project and region from gcloud config
    if region is None or project is None:
        gcp_config = get_gcloud_config()
        if project is None:
            project = get_gcp_project(gcp_config)
        if region is None:
            region = get_gcp_region(gcp_config)

    client_options = {"api_endpoint": get_endpoint_str(region)}
    client = aiplatform.PersistentResourceServiceClient(client_options=client_options)

    request = aiplatform.DeletePersistentResourceRequest(
        name=f"projects/{project}/locations/{region}/persistentResources/{persistent_resource_id}"
    )

    response = {}
    try:
        operation = client.delete_persistent_resource(request=request)
        print("Waiting for operation to complete...")
        response = operation.result()
    except Exception as e:
        print(e)
    return response
