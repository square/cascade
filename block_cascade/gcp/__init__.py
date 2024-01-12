from abc import ABC, abstractmethod
from typing import Optional

import requests


class VertexAIEnvironmentInfoProvider(ABC):
    """
    Abstract Base Class for obtaining values
    necessary for a Vertex AI Training Custom Job.
    """

    @abstractmethod
    def get_container_image(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_network(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_project(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_region(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_service_account(self) -> Optional[str]:
        pass


class VMMetadataServerClient(VertexAIEnvironmentInfoProvider):
    """
    A client for interacting with the metadata server
    for a GCP virtual machine.
    """

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({"Metadata-Flavor": "Google"})

    def get_container_image(self) -> Optional[str]:
        response = self._session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/attributes/",
            params={"recursive": True},
        )
        response.raise_for_status()

        instance_attributes = response.json()
        return instance_attributes["container"]

    def get_network(self) -> Optional[str]:
        response = self._session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/network"
        )
        response.raise_for_status()
        _, project, _, network = response.text.split("/")
        return f"projects/{project}/global/networks/{network}"

    def get_project(self) -> Optional[str]:
        response = self._session.get(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id"
        )
        response.raise_for_status()
        return response.text

    def get_region(self) -> Optional[str]:
        response = self._session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/zone"
        )
        response.raise_for_status()
        # Response is in format projects/{project_id}/zones/{region}-{zone}
        return response.text.split("/").pop().rsplit("-", maxsplit=1)[0]

    def get_service_account(self) -> Optional[str]:
        response = self._session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
        )
        response.raise_for_status()
        return response.text
