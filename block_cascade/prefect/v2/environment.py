from typing import Optional

from prefect.context import FlowRunContext

from block_cascade.concurrency import run_async
from block_cascade.gcp import VertexAIEnvironmentInfoProvider
from block_cascade.utils import PREFECT_SUBVERSION

if PREFECT_SUBVERSION <= 7:
    from prefect.orion.schemas.core import BlockDocument
else:
    from prefect.server.schemas.core import BlockDocument

from prefect.client.schemas.responses import DeploymentResponse

from block_cascade.prefect.v2 import _fetch_block, _fetch_deployment


class PrefectEnvironmentClient(VertexAIEnvironmentInfoProvider):
    """
    A client for fetching Deployment related
    metadata from a Prefect 2 Flow.
    """

    def __init__(self):
        self._current_deployment = None
        self._current_infrastructure = None

    def get_container_image(self) -> Optional[str]:
        infra = self._get_infrastructure_block()
        if not infra:
            return

        deployment_details = infra.data
        return deployment_details.get("image")

    def get_network(self) -> Optional[str]:
        infra = self._get_infrastructure_block()
        if not infra:
            return

        deployment_details = infra.data
        return deployment_details.get("network")

    def get_project(self) -> Optional[str]:
        infra = self._get_infrastructure_block()
        if not infra:
            return

        deployment_details = infra.data
        return deployment_details.get("gcp_credentials", {}).get("project")

    def get_region(self) -> Optional[str]:
        infra = self._get_infrastructure_block()
        if not infra:
            return

        deployment_details = infra.data
        return deployment_details.get("region")

    def get_service_account(self) -> Optional[str]:
        infra = self._get_infrastructure_block()
        if not infra:
            return

        deployment_details = infra.data
        return deployment_details.get("service_account")

    def _get_infrastructure_block(self) -> Optional[BlockDocument]:
        current_deployment = self._get_current_deployment()
        if not current_deployment:
            return None

        if not self._current_infrastructure:
            self._current_infrastructure = run_async(
                _fetch_block(current_deployment.infrastructure_document_id)
            )
        return self._current_infrastructure

    def _get_current_deployment(self) -> Optional[DeploymentResponse]:
        flow_context = FlowRunContext.get()
        if (
            not flow_context
            or not flow_context.flow_run
            or not flow_context.flow_run.deployment_id
        ):
            return None

        if not self._current_deployment:
            self._current_deployment = run_async(
                _fetch_deployment(flow_context.flow_run.deployment_id)
            )
        return self._current_deployment
