def detect_prefect_version(n) -> int:
    import prefect
    return int(prefect.__version__.split(".")[n])

PREFECT_VERSION, PREFECT_SUBVERSION = detect_prefect_version(0), detect_prefect_version(
    1
)

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
