from functools import partial, wraps
from importlib.metadata import version
from pathlib import Path
from typing import Callable, Optional, Union

import requests

from block_cascade.config import find_default_configuration
from block_cascade.gcp import VMMetadataServerClient
from block_cascade.executors.databricks.resource import DatabricksResource
from block_cascade.executors.databricks.executor import DatabricksExecutor
from block_cascade.executors.local.executor import LocalExecutor
from block_cascade.executors.vertex.executor import VertexExecutor
from block_cascade.executors.vertex.resource import GcpEnvironmentConfig, GcpResource
from block_cascade.executors.vertex.tune import Tune
from block_cascade.prefect import (
    PrefectEnvironmentClient,
    get_from_prefect_context,
    get_prefect_logger,
    is_prefect_cloud_deployment,
)
from block_cascade.utils import _infer_base_module, wrapped_partial

RESERVED_ARG_PREFIX = "remote_"


def remote(
    func: Union[Callable, partial, None] = None,
    resource: Union[GcpResource, DatabricksResource] = None,
    config_name: Optional[str] = None,
    job_name: Optional[str] = None,
    web_console_access: Optional[bool] = False,
    tune: Optional[Tune] = None,
    code_package: Optional[Path] = None,
    *args,
    **kwargs,
):
    """
    Decorator factory to generate a decorator that can be used to run a
    function remotely.

    @tasks
    @remote(resource=GcpResource(...))
    def train_model():
        ...

    The function (along with captured arguments) will then run as a separate vertex job
    on the specified resource.

    Some parameters to the remote function that cannot be determined at function
    decoration time can also be passed in during function run time, through keyword
    args with the convention `remote_{arg}` (i.e. to overwrite `tune`, set
    `remote_tune` in the function).

    @task
    @remote()
    def train_model(remote_tune=Tune(...))
        ...

    Parameters
    ----------
    func: Function
        The function to run remotely
    resource: Union[GcpResource, DatabricksResource]
        A description of the remote resource (environment and cluster configuration)
        to run the function on
    config_name: Optional[str]
        The name of the configuration to use; must be a named block in cascade.yml
        If not provided, the job_name will be used to key the configuration
    job_name: Optional[str]
        The display name for the job; if no name is passed (default) this will
        be inferred from the function name and environment
    web_console_access: Optional[bool]
        For VertexAI jobs in GCP, whether to allow web console access to the job
    tune: Optional[Tune]
        An optional Tune object to use for hyperparameter tuning; only on VertexAI
    code_package: Optional[Path]
        An optional path to the first party code that your remo.
        This is only necessary if the following conditions hold true:
            - The function is desired to run in Vertex AI
            - The function is not being executed from a Prefect2 Cloud Deployment
            - The function references a module that is not from a third party
            dependency, but from the same package the function is a member of.
    """

    if not resource:
        resource_configurations = find_default_configuration() or {}
        if config_name:
            resource = resource_configurations.get(config_name)
        else:
            resource = resource_configurations.get(job_name)

    remote_args = locals()
    # Support calling this with arguments before using as a decorator, e.g. this
    # allows us to do
    #
    # remote = remote(resource=GcpResource(...))
    #     ...
    # @remote
    # def train_model():
    #     ...s
    if func is None:
        return partial(
            remote,
            job_name=job_name,
            web_console_access=web_console_access,
            resource=resource,
            tune=tune,
            code_package=code_package,
            *args,
            **kwargs,
        )

    @wraps(func)
    def remote_func(*args, **kwargs):
        """
        Remote function that will be returned by the decorator factory.
        Will inherit the docstring and name of the function it decorates.
        """

        # list of parameters that can be overriden by supplying their value at
        # function call time as a keyword argument with the prefix "remote_"
        nonlocal remote_args
        for parameter in remote_args:
            if f"{RESERVED_ARG_PREFIX}{parameter}" in kwargs:
                remote_args[parameter] = kwargs[f"{RESERVED_ARG_PREFIX}{parameter}"]
                kwargs.pop(f"{RESERVED_ARG_PREFIX}{parameter}", None)

        resource = remote_args.get("resource", None)
        job_name = remote_args.get("job_name", None)
        tune = remote_args.get("tune", None)
        code_package = remote_args.get("code_package", None)
        web_console_access = remote_args.get("web_console_access", False)

        # get the prefect logger and flow metadata if available
        # to determine if this flow is running on the cloud
        prefect_logger = get_prefect_logger(__name__)

        flow_id = get_from_prefect_context("flow_id", "LOCAL")
        flow_name = get_from_prefect_context("flow_name", "LOCAL")
        task_id = get_from_prefect_context("task_run_id", "LOCAL")
        task_name = get_from_prefect_context("task_run", "LOCAL")

        via_cloud = is_prefect_cloud_deployment()
        prefect_logger.info(f"Via cloud? {via_cloud}")

        # create a new wrapped partial function with the passed *args and **kwargs
        # so that it can be sent to the remote executor with its parameters
        packed_func = wrapped_partial(func, *args, **kwargs)

        # if no resource is passed, run locally
        if resource is None:
            prefect_logger.info("Executing task with LocalExecutor.")
            executor = LocalExecutor(func=packed_func)

        # if a GcpResource is passed, try to run on Vertex
        elif isinstance(resource, GcpResource):
            prefect_logger.info("Executing task with GcpResource.")
            # Align naming with labels defined by Prefect
            # Infrastructure: https://github.com/PrefectHQ/prefect/blob/main/src/prefect/infrastructure/base.py#L134
            # and mutated to be GCP compatible: https://github.com/PrefectHQ/prefect-gcp/blob/main/prefect_gcp/aiplatform.py#L214
            labels = {
                "prefect-io_flow-run-id": flow_id,
                "prefect-io_flow-name": flow_name,
                "prefect-io_task-name": task_name,
                "prefect-io_task-id": task_id,
                "block_cascade-version": version("block_cascade"),
            }
            resource.environment = resource.environment or GcpEnvironmentConfig()
            if resource.environment.is_complete:
                executor = VertexExecutor(
                    resource=resource,
                    name=job_name,
                    func=packed_func,
                    tune=tune,
                    labels=labels,
                    logger=prefect_logger,
                    code_package=code_package,
                    web_console=web_console_access,
                )
            else:
                client = (
                    PrefectEnvironmentClient()
                    if via_cloud
                    else VMMetadataServerClient()
                )

                try:
                    if not resource.environment.image:
                        resource.environment.image = client.get_container_image()
                    if not resource.environment.project:
                        resource.environment.project = client.get_project()
                    if not resource.environment.service_account:
                        resource.environment.service_account = (
                            client.get_service_account()
                        )
                    if not resource.environment.region:
                        resource.environment.region = client.get_region()
                except requests.exceptions.ConnectionError:
                    prefect_logger.warning(
                        "Failure to connect to host. "
                        "Execution environment must be outside of "
                        "a GCP VM or as a result of Prefect "
                        "Deployment."
                    )

                if not resource.environment.is_complete:
                    raise RuntimeError(
                        "Unable to infer remaining environment for GcpResource. "
                        "Please provide a complete environment to the "
                        "configured GcpResource."
                    )
                executor = VertexExecutor(
                    resource=resource,
                    func=packed_func,
                    name=job_name,
                    tune=tune,
                    labels=labels,
                    logger=prefect_logger,
                    code_package=code_package,
                    web_console=web_console_access,
                )
        elif (
            isinstance(resource, DatabricksResource)
            or "DatabricksResource" in type(resource).__name__
        ):
            prefect_logger.info("Executing task with DatabricksResource.")
            failed_to_infer_base = (
                "Unable to infer base module of function. Specify "
                "the base module in the `cloud_pickle_by_value` attribute "
                "of the DatabricksResource object if necessary."
            )
            if resource.cloud_pickle_infer_base_module:
                base_module_name = _infer_base_module(func)
                # if base module is __main__ or None, it can't be registered
                if base_module_name is None or base_module_name.startswith("__"):
                    prefect_logger.warn(failed_to_infer_base)
                else:
                    resource.cloud_pickle_by_value.append(base_module_name)

            executor = DatabricksExecutor(
                func=packed_func,
                resource=resource,
                name=job_name,
            )
        else:
            raise ValueError("No valid resource provided.")

        # if sucessful, this will return the result of <executor>._result()
        return executor.run()

    return remote_func
