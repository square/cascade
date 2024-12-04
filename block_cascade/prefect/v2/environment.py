from typing import Dict, Optional

from prefect import runtime

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
        self._current_job_variables = None
        self._current_infrastructure = None

    def get_container_image(self) -> Optional[str]:
        job_variables = self._get_job_variables()
        if job_variables:
            return job_variables.get("image")

        infra = self._get_infrastructure_block()
        if infra:
            return infra.data.get("image")
        return None

    def get_network(self) -> Optional[str]:
        job_variables = self._get_job_variables()
        if job_variables:
            return job_variables.get("network")

        infra = self._get_infrastructure_block()
        if infra:
            return infra.data.get("network")

        return None

    def get_project(self) -> Optional[str]:
        job_variables = self._get_job_variables()
        if job_variables:
            return job_variables.get("credentials", {}).get("project")

        infra = self._get_infrastructure_block()
        if infra:
            return infra.data.get("gcp_credentials", {}).get("project")

        return None

    def get_region(self) -> Optional[str]:
        job_variables = self._get_job_variables()
        if job_variables:
            return job_variables.get("region")

        infra = self._get_infrastructure_block()
        if infra:
            return infra.data.get("region")

        return None

    def get_service_account(self) -> Optional[str]:
        job_variables = self._get_job_variables()
        if job_variables:
            return job_variables.get("service_account_name")

        infra = self._get_infrastructure_block()
        if infra:
            return infra.data.get("service_account")

        return None

    def _get_job_variables(self) -> Optional[Dict]:
        current_deployment = self._get_current_deployment()
        if not current_deployment:
            return None

        if not self._current_job_variables:
            self._current_job_variables = current_deployment.job_variables
        return self._current_job_variables

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
        deployment_id = runtime.deployment.id
        if not deployment_id:
            return None

        if not self._current_deployment:
            self._current_deployment = run_async(
                _fetch_deployment(deployment_id)
            )
        return self._current_deployment
