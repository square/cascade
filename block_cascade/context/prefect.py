from importlib.metadata import version
from typing import Optional

from block_cascade.context import Context
from block_cascade.prefect import (
    PrefectEnvironmentClient,
    get_from_prefect_context,
)

class PrefectContext(Context):
    """
    Represents the context from a Prefect Flow Run from a Prefect
    Deployment.
    """

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def __init__(self):
        self._client = PrefectEnvironmentClient()

    def tags(self) -> dict[str, str]:
        # Align naming with labels defined by Prefect
        # Infrastructure: https://github.com/PrefectHQ/prefect/blob/main/src/prefect/infrastructure/base.py#L134
        # and mutated to be GCP compatible: https://github.com/PrefectHQ/prefect-gcp/blob/main/prefect_gcp/aiplatform.py#L214
        flow_id = get_from_prefect_context("flow_id", "LOCAL")
        flow_name = get_from_prefect_context("flow_name", "LOCAL")
        task_id = get_from_prefect_context("task_run_id", "LOCAL")
        task_name = get_from_prefect_context("task_run", "LOCAL")
        labels = {
            "prefect-io_flow-run-id": flow_id,
            "prefect-io_flow-name": flow_name,
            "prefect-io_task-name": task_name,
            "prefect-io_task-id": task_id,
            "block_cascade-version": version("block_cascade"),
        }
        return labels

    def image(self) -> Optional[str]:
        return self._client.get_container_image()

    def project(self) -> Optional[str]:
        return self._client.get_project()

    def service_account(self) -> Optional[str]:
        return self._client.get_service_account()

    def region(self) -> Optional[str]:
        return self._client.get_region()