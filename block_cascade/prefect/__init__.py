from importlib.util import find_spec

if find_spec("prefect") is None:
    raise ImportError("Requires the `prefect` extra.")

from block_cascade.prefect.version import PREFECT_VERSION

if PREFECT_VERSION == 2:
    from .v2 import (
        get_from_prefect_context,
        get_prefect_logger,
        is_prefect_cloud_deployment,
    )
    from .v2.environment import PrefectEnvironmentClient
else:
    from .v3 import (
        get_from_prefect_context,
        get_prefect_logger,
        is_prefect_cloud_deployment,
    )
    from .v3.environment import PrefectEnvironmentClient
