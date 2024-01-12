import os
import sys
from typing import Optional

import google.auth
from google.cloud import resourcemanager_v3
from prefect.backend.flow import FlowView

from block_cascade.gcp import VertexAIEnvironmentInfoProvider
from block_cascade.prefect.v1 import get_from_prefect_context


class PrefectEnvironmentClient(VertexAIEnvironmentInfoProvider):
    """
    A client for fetching Deployment related
    metadata from a Prefect 1 Flow.
    """

    def __init__(self):
        super().__init__()
        self._flow_id = get_from_prefect_context("flow_id")

    def get_container_image(self) -> Optional[str]:
        if not self._flow_id:
            return None

        flow_view = FlowView.from_flow_id(self._flow_id)
        registry_url = flow_view.storage.registry_url
        image_tag = flow_view.storage.image_tag
        image_name = flow_view.storage.image_name
        image_url = f"{registry_url}/{image_name}:{image_tag}"
        return image_url

    def get_network(self) -> Optional[str]:
        if not self._flow_id:
            return None

        flow_view = FlowView.from_flow_id(self._flow_id)
        if flow_view:
            return getattr(flow_view.run_config, "network", None)
        return None

    def get_project(self) -> Optional[str]:
        # Set manually in deployed Docker image
        project_name = os.environ.get("GCP_PROJECT")
        # Set by Vertex training job
        project_number = os.environ.get("CLOUD_ML_PROJECT_ID")

        if project_name:
            return project_name

        elif project_number:
            client = resourcemanager_v3.ProjectsClient()
            request = resourcemanager_v3.GetProjectRequest(
                name=f"projects/{project_number}"
            )
            response = client.get_project(request=request)
            return response.project_id

        elif sys.platform == "darwin":  # Only works locally - not on Vertex
            _, project = google.auth.default()
            return project

        else:
            # protect against any edge cases where project cannot be determined
            # via other methods
            raise google.auth.exceptions.DefaultCredentialsError(
                "Project could not be determined from environment"
            )

    def get_region(self) -> Optional[str]:
        if not self._flow_id:
            return None
        flow_view = FlowView.from_flow_id(self._flow_id)
        if flow_view:
            return getattr(flow_view.run_config, "region", None)
        return None

    def get_service_account(self) -> Optional[str]:
        if not self._flow_id:
            return

        flow_view = FlowView.from_flow_id(self._flow_id)
        if flow_view:
            service_account = getattr(flow_view.run_config, "service_account", None)

        # Check environment for service account.
        # It will injected by Google later.
        service_account = (
            service_account
            or os.getenv("SERVICE_ACCOUNT")
            or os.getenv("CLOUD_ML_JOB_SA")
        )
        return service_account
