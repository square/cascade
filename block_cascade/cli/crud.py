import logging
from typing import Optional

import click
import json
from google.cloud import aiplatform_v1beta1 as aiplatform
from google.cloud.aiplatform_v1beta1 import ServiceAccountSpec, ResourceRuntimeSpec
from google.protobuf import json_format

from block_cascade import GcpResource
from block_cascade.config import find_default_configuration
from block_cascade.consts import SERVICE
from block_cascade.utils import get_gcloud_config

logger = logging.getLogger(__name__)


def get_gcp_project(config: dict) -> str:
    """Return the GCP project from a local config."""
    return config.get("core", {})["project"]


def get_gcp_region(config: dict) -> str:
    """Return the GCP region from a local config."""
    return config.get("compute", {})["region"]


def get_persistent_resource_payload(resource: GcpResource) -> dict:
    """Generate a persistent resource payload from a GcpResource."""

    # service account spec
    resource_runtime_spec = ResourceRuntimeSpec(
        service_account_spec=ServiceAccountSpec(enable_custom_service_account=True)
    )

    cpu_machine_type = resource.chief.type
    cpu_replica_count = resource.chief.count
    cpu_min_replica_count = resource.chief.min_replica_count or 0
    cpu_max_replica_count = resource.chief.max_replica_count or 0
    if resource.chief.accelerator is not None:
        gpu_machine_type = resource.chief.type
        gpu_replica_count = resource.chief.count
        gpu_accelerator_type = resource.chief.accelerator.type
        gpu_accelerator_count = resource.chief.accelerator.count
    else:
        gpu_machine_type = None
        gpu_replica_count = 0
        gpu_accelerator_type = None
        gpu_accelerator_count = 0

    # This is consistent with the default disk spec of jobs.
    disk_spec = {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100}

    cpu_pool = {
        "machine_spec": {
            "machine_type": cpu_machine_type,
        },
        "replica_count": cpu_replica_count,
        "disk_spec": disk_spec,
    }
    if cpu_min_replica_count > 0 or cpu_max_replica_count > 0:
        cpu_pool["autoscaling_spec"] = {
            "min_replica_count": cpu_min_replica_count,
            "max_replica_count": cpu_max_replica_count,
        }

    gpu_pool = {
        "machine_spec": {
            "machine_type": gpu_machine_type,
            "accelerator_type": gpu_accelerator_type,
            "accelerator_count": gpu_accelerator_count,
        },
        "replica_count": gpu_replica_count,
        "disk_spec": disk_spec,
    }

    if cpu_replica_count > 0 and gpu_replica_count > 0:
        RESOURCE_POOLS = [cpu_pool, gpu_pool]
    elif cpu_replica_count > 0:
        RESOURCE_POOLS = [cpu_pool]
    elif gpu_replica_count > 0:
        RESOURCE_POOLS = [gpu_pool]
    else:
        logger.error(
            "No CPU or GPU replicas specified, persistent resources can not be created."
        )

    persistent_resource = {
        "display_name": resource.persistent_resource_id,
        "resource_pools": RESOURCE_POOLS,
        "resource_runtime_spec": resource_runtime_spec,
    }

    return persistent_resource


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
    "--config-name",
    "-c",
    required=True,
    help="Name of the configuration block in cascade.yml to use.",
)
def create_persistent_resource(config_name: str):
    """
    Create a persistent resource from a configuration block in cascade.yaml.
    Configuration block will be keyed on the config-name argument.
    """
    resource = find_default_configuration()[config_name]
    persistent_resource = get_persistent_resource_payload(resource)

    # CreatePersistentResoure SDK
    region = resource.environment.region
    client_options = {"api_endpoint": f"{region}-{SERVICE}"}
    client = aiplatform.PersistentResourceServiceClient(client_options=client_options)

    parent = get_parent_str(resource)

    try:
        operation = client.create_persistent_resource(
            parent=parent,
            persistent_resource_id=resource.persistent_resource_id,
            persistent_resource=persistent_resource,
        )
        click.echo(
            """
            Attempting to create Persistent Resource... 
            This may take several minutes and will continue if you close this terminal.
            """
        )
        response = operation.result()
        response = json.loads(json_format.MessageToJson(response._pb))
        click.echo(f'Persistent resource {response["name"]} created successfully.')
    except Exception as e:
        click.echo(e)


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
def list_persistent_resources(
    region: Optional[str] = None, project: Optional[str] = None
):
    # try to infer project and region from gcloud config
    if region is None or project is None:
        gcp_config = get_gcloud_config()
        if project is None:
            project = get_gcp_project(gcp_config)
        if region is None:
            region = get_gcp_region(gcp_config)
        if project is None or project == "":
            click.echo(
                "Could not infer project from gcloud config. Please provide directly to the CLI."
            )
            return
        if region is None or region == "":
            click.echo(
                "Could not infer region from gcloud config. Please provide directly to the CLI."
            )
            return

    client_options = {"api_endpoint": f"{region}-{SERVICE}"}
    client = aiplatform.PersistentResourceServiceClient(client_options=client_options)

    click.echo("Querying the list of Persistent Resources...")
    request = aiplatform.ListPersistentResourcesRequest(
        parent=f"projects/{project}/locations/{region}"
    )

    response = client.list_persistent_resources(request=request)
    response_json = json.loads(json_format.MessageToJson(response._pb))
    click.echo("Listing Persistent Resources:\n")
    click.echo(json.dumps(response_json, indent=2))


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
    persistent_resource_id: str,
    region: Optional[str] = None,
    project: Optional[str] = None,
):
    # try to infer project and region from gcloud config
    if region is None or project is None:
        gcp_config = get_gcloud_config()
        if project is None:
            project = get_gcp_project(gcp_config)
        if project is None or project == "":
            click.echo(
                "Could not infer project from gcloud config. Please provide directly to the CLI."
            )
            return
        if region is None:
            region = get_gcp_region(gcp_config)
        if region is None or region == "":
            click.echo(
                "Could not infer region from gcloud config. Please provide directly to the CLI."
            )
            return

    client_options = {"api_endpoint": get_endpoint_str(region)}
    client = aiplatform.PersistentResourceServiceClient(client_options=client_options)

    request = aiplatform.DeletePersistentResourceRequest(
        name=f"projects/{project}/locations/{region}/persistentResources/{persistent_resource_id}"
    )
    try:
        operation = client.delete_persistent_resource(request=request)
        click.echo("Waiting for operation to complete...")
        _ = operation.result()
        click.echo("Persistent resource deleted successfully.")
    except Exception as e:
        click.echo(e)


@click.command()
@click.option(
    "--id",
    "-i",
    required=True,
    help="ID of the persistent resource to describe.",
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
def list_active_jobs(
    id: str, project: Optional[str] = None, region: Optional[str] = None
):
    client_options = {"api_endpoint": get_endpoint_str(region)}
    client = aiplatform.JobServiceClient(client_options=client_options)

    request = aiplatform.ListCustomJobsRequest(
        parent=f"projects/{project}/locations/{region}",
        filter='(state!="JOB_STATE_SUCCEEDED" AND state!="JOB_STATE_FAILED" AND state!="JOB_STATE_CANCELLED") AND labels.presistent_resource_job=true',
    )

    page_result = client.list_custom_jobs(request=request)
    persistentResourceJobs = {"customJobs": []}
    jobs_json = json.loads(json_format.MessageToJson(page_result._pb))

    try:
        customJobs = jobs_json["customJobs"]
        persistentResourceJobs = {"customJobs": []}
    except:
        customJobs = {}

    for customJob in customJobs:
        try:
            persistentResourceId = customJob["jobSpec"]["persistentResourceId"]
        except:
            continue
        if id == persistentResourceId:
            persistentResourceJobs["customJobs"] += [customJob]

    click.echo(persistentResourceJobs)
