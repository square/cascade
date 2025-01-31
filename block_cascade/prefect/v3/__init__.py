import logging
from typing import Optional, Union

import prefect
from prefect import get_run_logger
from prefect import runtime
from prefect.client.orchestration import get_client
from prefect.client.schemas.responses import DeploymentResponse
from prefect.context import FlowRunContext, TaskRunContext
from prefect.server.schemas.core import BlockDocument

from block_cascade.concurrency import run_async


_CACHED_DEPLOYMENT: Optional[DeploymentResponse] = None
_CACHED_STORAGE: Optional[BlockDocument] = None


async def _fetch_deployment(deployment_id: str) -> BlockDocument:
    async with get_client() as client:
        return await client.read_deployment(deployment_id)

async def _fetch_block_by_name(block_name: str, block_type_slug: str = "gcs-bucket") -> Optional[BlockDocument]:
    async with get_client() as client:
        return await client.read_block_document_by_name(
            name=block_name,
            block_type_slug=block_type_slug,
        )

def get_from_prefect_context(attr: str, default: str = "") -> str:
    flow_context = FlowRunContext.get()
    task_context = TaskRunContext.get()
    if not flow_context or not task_context:
        return default

    if attr == "flow_name" or attr == "flow_run_name":  # noqa: PLR1714
        return str(getattr(flow_context.flow_run, "name", default))
    if attr == "flow_id" or attr == "flow_run_id":  # noqa: PLR1714
        return str(getattr(flow_context.flow_run, "id", default))
    if attr == "task_run" or attr == "task_full_name":  # noqa: PLR1714
        return str(getattr(task_context.task_run, "name", default))
    if attr == "task_run_id":
        return str(getattr(task_context.task_run, "id", default))
    raise RuntimeError("Unsupported attribute: {attr}.")


def get_current_deployment() -> Optional[DeploymentResponse]:
    deployment_id = runtime.deployment.id
    if not deployment_id:
        return None

    global _CACHED_DEPLOYMENT  # noqa: PLW0603
    if not _CACHED_DEPLOYMENT:
        _CACHED_DEPLOYMENT = run_async(
            _fetch_deployment(deployment_id)
        )
    return _CACHED_DEPLOYMENT


def get_storage_block() -> Optional[BlockDocument]:
    current_deployment = get_current_deployment()
    if not current_deployment:
        return None

    global _CACHED_STORAGE  # noqa: PLW0603
    if not _CACHED_STORAGE:
        _CACHED_STORAGE = run_async(
            _fetch_block_by_name(block_name=current_deployment.pull_steps[0]["prefect.deployments.steps.pull_with_block"]["block_document_name"])
        )
    return _CACHED_STORAGE


def get_prefect_logger(name: str = "") -> Union[logging.LoggerAdapter, logging.Logger]:
    """
    Tries to get the prefect run logger, and if it is not available,
    gets the root logger.
    """
    try:
        return get_run_logger()
    except prefect.exceptions.MissingContextError:
        # if empty string is passed,
        # obtains access to the root logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        return logger


def is_prefect_cloud_deployment() -> bool:
    return runtime.deployment.id is not None
