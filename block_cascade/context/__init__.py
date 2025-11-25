import os
from typing import Optional

class Context:
    """
    Represents the context of an execution to infer default values
    for submitting remote executions.
    """

    def __new__(cls, *args, **kwargs):
        try:
            from block_cascade.context.prefect import PrefectContext
            from prefect import get_run_logger
            from prefect.exceptions import MissingContextError
            get_run_logger()
            return super().__new__(PrefectContext)
        except MissingContextError:
            if "VERTEX_PRODUCT" in os.environ:
                from block_cascade.context.gcp import VMMetadataServer
                return super().__new__(VMMetadataServer)
            return super().__new__(cls)
        except ImportError:
            return super().__new__(cls)

    def tags(self) -> dict[str, str]:
        """
        Returns a dictionary of tags from the context.
        """
        return {}

    def image(self) -> Optional[str]:
        """
        Returns the image name from the context.
        """
        return None

    def project(self) -> Optional[str]:
        """
        Returns the project name from the context.
        """
        return None

    def service_account(self) -> Optional[str]:
        """
        Returns the service account from the context.
        """
        return None

    def region(self) -> Optional[str]:
        """
        Returns the region from the context.
        """
        return None

    def kind(self) -> str:
        """
        Returns the kind of the context.
        """
        return self.__class__.__name__