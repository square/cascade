def detect_prefect_version(n) -> int:
    import prefect
    return int(prefect.__version__.split(".")[n])

PREFECT_VERSION, PREFECT_SUBVERSION = detect_prefect_version(0), detect_prefect_version(
    1
)