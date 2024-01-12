from asyncio import gather
from collections import defaultdict
from datetime import datetime
from enum import Enum

from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import types as monitoring_types

from block_cascade.executors.vertex.resource import GcpResource
from block_cascade.prefect import get_prefect_logger

SERVICE = "aiplatform.googleapis.com"
RESOURCE_CATEGORY = "custom_model_training"


class GcpMetrics(Enum):
    QUOTA_LIMIT = ("serviceruntime.googleapis.com/quota/limit", 86400 * 2)
    QUOTA_USAGE = ("serviceruntime.googleapis.com/quota/allocation/usage", 86400 * 2)

    def __init__(self, metric_type, delta):
        self.metric_type = metric_type
        self.interval = self.get_interval(delta)

    @staticmethod
    def get_interval(delta: int) -> monitoring_v3.TimeInterval:
        """
        Produce a time interval

        Args:
            delta (int): amount of time (in seconds) in the past to subtract
            from the current time,this is adjusted based on metric type as each
            metric is updated at different intervals
            see: https://cloud.google.com/monitoring/api/metrics_gcp#gcp-serviceruntime

        Returns:
            monitoring_v3.TimeInterval
        """
        start = datetime.utcnow()
        start_seconds = int(start.strftime("%s"))
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": start_seconds, "nanos": 0},
                "start_time": {"seconds": (start_seconds - delta), "nanos": 0},
            }
        )
        return interval


def _create_quota_metric_suffix_from_cpu_type(machine_type: str) -> str:
    """
    Generate a query filter to determine quota/usage for a given cpu type

    Args:
        machine_type (str): cpu type supplied by end user in GcpMachineConfig

    Raises:
        Key error, if cpu type not supported

    Returns:
        str: a metric.label.quota_metric for querying the monitoring API
    """
    cpu_to_quota = {
        "a2": "a2_cpus",
        "n1": "cpus",
        "n2": "n2_cpus",
        "c2": "c2_cpus",
        "m1": "m1_cpus",
        "g2": "g2_cpus",
    }

    quota_type = cpu_to_quota[machine_type]

    return quota_type


def _create_quota_metric_suffix_from_gpu_type(accelerator_type: str) -> str:
    """
    Generate a query filter to determine quota/usage for a given accelerator

    Args:
        accelerator_type (str): accelerator supplied by end user in GcpAcceleratorConfig

    Raises:
        e: Key error, if accelerator type not supported

    Returns:
        str: a metric.label.quota_metric for querying the monitoring API
    """

    return accelerator_type.lower() + "_gpus"


def _get_most_recent_point(
    time_series: monitoring_types.metric.TimeSeries,
) -> str:
    """
    Return the most end_time timestamp and quota value
    from a TimeSeries object.
    """
    try:
        quota_val = time_series.points[0].value.int64_value
    except Exception:
        return "unknown"
        # if limit > 1*10^6, unlimited

    if quota_val > 10**6:
        return "unlimited"

    return str(quota_val)


async def _make_metric_request(
    project: str, metric_type: GcpMetrics, quota_metric: str, region: str, logger
) -> str:
    """
    Create a MetricServiceAsyncClient and try making a call to list time series for
    the given metric, if an error is encountered return a missing list.
    Args:
        project (str):
        metric_type (GcpMetrics):
        quota_metric (str):
        region (str):

    Returns:
        List[monitoring_types.metric.TimeSeries]: Return a list of Time series,
        if an error is encountered return an empty list
    """
    try:
        client = monitoring_v3.MetricServiceAsyncClient()
    except Exception as e:
        logger.error(e)
        return []

    try:
        results = await client.list_time_series(
            # the request should be specified so that it returns only one metric
            request={
                "name": f"projects/{project}",
                "filter": f'metric.type = "{metric_type.metric_type}" AND\
                      metric.label.quota_metric="{quota_metric}" AND\
                          resource.labels.location="{region}"',
                "interval": metric_type.interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )
    except Exception as e:
        logger.error(e)
        return []

    results_list = []

    async for page in results:
        results_list.append(page)

    if len(results_list) == 0:
        return "unknown"

    return _get_most_recent_point(results_list[0])


def _get_resources_by_metric(resource: GcpResource) -> dict:
    """
    Create a dictionary to store all resource types in a
    GcpResource object and num of each resource

    Args:
        resource (GcpResource): complete GcpResource obj

    Returns:
        dict: keys are quota_metric, values are number of that type of resource
    """
    resources_by_metric = defaultdict(lambda: 0)

    cpu_resources = [resource.chief]
    if resource.workers:
        cpu_resources.append(resource.workers)

    gpu_resources = []
    if resource.chief.accelerator:
        gpu_resources.append(resource.chief.accelerator)
    if resource.workers and resource.workers.accelerator:
        gpu_resources.append(resource.workers.accelerator)

    # cpus
    for pool in cpu_resources:
        cpu_type, _, num_cores = str.split(pool.type, "-")
        resources_by_metric[_create_quota_metric_suffix_from_cpu_type(cpu_type)] += (
            int(num_cores) * pool.count
        )

    # gpus
    for accelerator in gpu_resources:
        resources_by_metric[
            _create_quota_metric_suffix_from_gpu_type(accelerator.type)
        ] += accelerator.count

    return resources_by_metric


async def log_quotas_for_resource(resource: GcpResource) -> None:
    """
    Parses all resources in a GcpResource object and queries GCP to determine
    current usage and quota limit for each resource type.

    Logs results to a Prefect run logger if available; gcp.monitoring logger if not.

    Args:
        resource (GcpResource)
    """
    logger = get_prefect_logger(__name__)

    resources_by_quota_metric = _get_resources_by_metric(resource)

    for quota_metric_suffix, num_resouces in resources_by_quota_metric.items():
        # create string of resource for logging
        metric_str = quota_metric_suffix
        if metric_str[-1] == "s":
            metric_str = metric_str[:-1]

        resource_log_str = (
            f"VertexJob will consume {num_resouces} {metric_str} resources."
        )

        # get quota limits
        limit_p = _make_metric_request(
            project=resource.environment.project,
            metric_type=GcpMetrics.QUOTA_LIMIT,
            quota_metric=f"{SERVICE}/{RESOURCE_CATEGORY}_{quota_metric_suffix}",
            region=resource.environment.region,
            logger=logger,
        )

        # get quota usage
        usage_p = _make_metric_request(
            project=resource.environment.project,
            metric_type=GcpMetrics.QUOTA_USAGE,
            quota_metric=f"{SERVICE}/{RESOURCE_CATEGORY}_{quota_metric_suffix}",
            region=resource.environment.region,
            logger=logger,
        )

        limit, usage = await gather(limit_p, usage_p)

        logger.info(resource_log_str + f"Current usage: {usage}; quota limit: {limit}.")
