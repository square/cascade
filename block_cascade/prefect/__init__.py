from block_cascade.utils import PREFECT_VERSION

if PREFECT_VERSION == 1:
    from .v1 import (
        get_from_prefect_context,
        get_prefect_logger,
        is_prefect_cloud_deployment,
    )
    from .v1.environment import PrefectEnvironmentClient
elif PREFECT_VERSION == 2:
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
