from typing import Optional

from block_cascade.context import Context
from block_cascade.gcp import VMMetadataServerClient

class VMMetadataServer(Context):
    """
    Represents the context from a GCP virtual machine
    by querying the metadata server for values.
    """

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def __init__(self):
        self._client = VMMetadataServerClient()

    def tags(self) -> dict[str, str]:
        return {}

    def project(self) -> Optional[str]:
        return self._client.get_project()

    def service_account(self) -> Optional[str]:
        return self._client.get_service_account()

    def region(self) -> Optional[str]:
        return self._client.get_region()

    def image(self) -> Optional[str]:
        return self._client.get_container_image()

