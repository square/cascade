import os
from typing import List, Optional, Union

from pydantic.dataclasses import Field, dataclass


@dataclass
class DatabricksSecret:
    """Databricks secret to auth to Databricks

    Parameters
    ----------
    host: str
    token: str
    """

    host: str
    token: str


@dataclass(frozen=True)
class DatabricksAutoscaleConfig:
    """Configuration for autoscaling on DataBricks clusters.

    Parameters
    ----------
    min_workers: Optional[int]
        Minimum number of workers to scale to. Default is 1.
    max_workers: Optional[int]
        Maximum number of workers to scale to. Default is 8.

    """

    min_workers: Optional[int] = 1
    max_workers: Optional[int] = 8


@dataclass()
class DatabricksResource:
    """Description of a Databricks Cluster

    Parameters
    ----------
    storage_location: str
       Path to the directory on s3 where files will be staged and output written
       cascade needs to have access to this bucket from the execution environment
    worker_count: Optional[Union[int, DatabricksAutoscaleConfig]]
        If an integer is supplied, specifies the of workers in Databricks cluster.
        If a `DatabricksAutoscaleConfig` is supplied, specifies the autoscale
        configuration to use. Default is 1 worker, not autoscaling.
    machine: Optional[str]
        AWS machine type for worker nodes. See https://www.databricks.com/product/aws-pricing/instance-types
        Default is i3.xlarge (4 vCPUs, 31 GB RAM)
    spark_version: Optional[str]
        Databricks runtime version. Tested on 11.3.x-scala2.12.
        https://docs.databricks.com/release-notes/runtime/releases.html
        Default is 11.3.x-scala2.12
    data_security_mode: Optional[str]
        See `data_security_mode` at
        https://docs.databricks.com/administration-guide/clusters/policies.html#cluster-policy-attribute-paths
        Sets Databricks security mode. At time of writing, Delta Live Tables require `SINGLE_USER` mode. In Cascade
        versions <=0.9.5, default was `NONE`.
    cluster_spec_overrides: Optional[dict]
        Additional entries to add to task `new_cluster` object in Databricks API call.
        https://docs.databricks.com/dev-tools/api/latest/clusters.html#request-structure-of-the-cluster-definition
        Example: {"spark_env_vars": {'A_VARIABLE': "A_VALUE"}}
    cluster_policy: Optional[str] = None
        Databricks cluster policy name (policy ID is looked up using this name). See
        https://docs.databricks.com/administration-guide/clusters/policies.html for details.
        By default, looks up `group_name`'s default cluster policy. Most users do
        not need to configure.
    existing_cluster_id: Optional[str] = None
        If specified, does not start a new cluster and instead attempts to deploy this job
        to an existing cluster with this ID. Useful during testing to avoid lag time of
        repeated cluster starts between iterations.
        https://docs.databricks.com/clusters/create-cluster.html
        Get cluster ID from JSON link within cluster info in Databricks UI.
    group_name: str
        The group name to run as in the databricks instance
        See "access_control_list"."group_name" in Databricks Job's API
        https://docs.databricks.com/api/workspace/jobs/create
    secret : Optional[DatabricksSecret]
        Token and hostname used to authenticate
        Required to run tasks on Databricks
    s3_credentials: dict
        Credentials to access S3, will be used to initialize S3FileSystem by
        calling s3fs.S3FileSystem(**s3_credentials),
        If no credentials are provided boto's credential resolver will be used.
        For details see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    cloud_pickle_by_value: list[str]
        List of names of modules to be pickled by value instead of by reference.
    cloudpickle_infer_base_module: Optional[bool] = True
        Whether to automatically infer the remote function's base module to be included
        in the `cloudpickle_by_value`
    task_args: Optional[dict] = None
        If provided, pass these additional arguments into the databricks task. This can
        be used
    python_libraries: Optional[List[str]] = None
        If provided, install these additional libraries on the cluster when the
        remote task is run

    """  # noqa: E501

    storage_location: str
    worker_count: Optional[Union[int, DatabricksAutoscaleConfig]] = 1
    machine: Optional[str] = "i3.xlarge"
    spark_version: Optional[str] = "11.3.x-scala2.12"
    data_security_mode: Optional[str] = "SINGLE_USER"
    cluster_spec_overrides: Optional[dict] = None
    cluster_policy: Optional[str] = None
    existing_cluster_id: Optional[str] = None
    group_name: Optional[str] = None
    secret: Optional[DatabricksSecret] = None
    environment: Optional[str] = "prod"
    s3_credentials: Optional[dict] = None
    cloud_pickle_by_value: Optional[List[str]] = Field(default_factory=list)
    cloud_pickle_infer_base_module: Optional[bool] = True
    task_args: Optional[dict] = None
    python_libraries: Optional[List[str]] = None

    def __post_init__(self):
        if self.group_name is None:
            self.group_name = os.environ.get("DATABRICKS_GROUP", "default-group")
