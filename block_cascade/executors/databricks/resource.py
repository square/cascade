import importlib.metadata
import logging
import os
from typing import Any, Iterator, List, Optional, Union

from pydantic import BaseModel, Field, model_validator


logger = logging.getLogger(__name__)

class DatabricksSecret(BaseModel):
    """Databricks secret to auth to Databricks

    Parameters
    ----------
    host: str
    token: str
    """

    host: str
    token: str


class DatabricksAutoscaleConfig(BaseModel):
    """Configuration for autoscaling on DataBricks clusters.

    Parameters
    ----------
    min_workers: Optional[int]
        Minimum number of workers to scale to. Default is 1.
    max_workers: Optional[int]
        Maximum number of workers to scale to. Default is 8.

    """

    min_workers: int = 1
    max_workers: int = 8


class DatabricksPythonLibrary(BaseModel):
    """Configuration for a Python library to be installed on a Databricks cluster.
    
    Reference: https://docs.databricks.com/aws/en/reference/jobs-2.0-api#pythonpypilibrary

    Parameters
    ----------
    name: str
        The name of the package.
    repo: Optional[str]
        The Python package index to install the package from. If not specified,
        defaults to the configured package index on the Databricks cluster which
        is most likely PyPI.
    version: Optional[str]
        The version of the package.
    infer_version: bool
        Whether to infer the version of the package from the current
        environment if not specified explicitly. Defaults to True.
        It is critical to recognize the Databricks runtime has preinstalled
        packages so version pinning can lead to an incompatible Databricks
        runtime.  Alternatively by not pinning, an incompatible version
        of the dependency could be installed that is not compatible with
        your code.
    """
    name: str
    repo: Optional[str] = None
    version: Optional[str] = None
    infer_version: bool = True

    @model_validator(mode="after")
    def maybe_update_version(self):
        if self.infer_version and not self.version:
            try:
                self.version = importlib.metadata.version(self.name)
            except importlib.metadata.PackageNotFoundError:
                logger.warning(
                    f"Could not infer version for package '{self.name}' from runtime. "
                    "The version will be left unspecified for Databricks runtime "
                    "installation."
                )
        return self

    def __str__(self) -> str:
        """Convert to pip requirement string format."""
        return f"{self.name}=={self.version}" if self.version else self.name

    def model_dump(self, **kwargs) -> dict:
        package_specififer = str(self)
        return {
            "pypi": {
                "package": package_specififer,
                **({"repo": self.repo} if self.repo else {})
            }
        }


class DatabricksResource(BaseModel):
    """Description of a Databricks Cluster

    Parameters
    ----------
    storage_location: str
        Path to the directory where files will be staged and output written.
        Storage location can be either Unity Catalog Volumes (/Volumes/) or S3 (s3://). Unity Catalog 
        is required for serverless compute.
    worker_count: Union[int, DatabricksAutoscaleConfig]
        If an integer is supplied, specifies the of workers in Databricks cluster.
        If a `DatabricksAutoscaleConfig` is supplied, specifies the autoscale
        configuration to use. Default is 1 worker without autoscaling enabled.
    machine: str
        AWS machine type for worker nodes. See https://www.databricks.com/product/aws-pricing/instance-types
        Default is i3.xlarge (4 vCPUs, 31 GB RAM)
    spark_version: Optional[str]
        Databricks runtime version.
        https://docs.databricks.com/release-notes/runtime/releases.html
        Required when use_serverless=False. Must not be set when use_serverless=True.
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
        calling s3fs.S3FileSystem(**s3_credentials).
        Required when storage_location starts with "s3://" (cluster compute).
        If no credentials are provided boto's credential resolver will be used.
        For details see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        Not needed for Unity Catalog Volumes (/Volumes/, serverless compute).
    cloud_pickle_by_value: list[str]
        List of names of modules to be pickled by value instead of by reference.
    cloudpickle_infer_base_module: bool = True
        Whether to automatically infer the remote function's base module to be included
        in the `cloudpickle_by_value`
    task_args: Optional[dict] = None
        If provided, pass these additional arguments into the databricks task. This can
        be used
    python_libraries: List[PythonLibrary] = [] 
        If provided, install these additional libraries on the cluster when the
        remote task is run.
    timeout_seconds: int = 86400
        The maximum time this job can run for; default is 24 hours.
    use_serverless: bool = False
        If True, use Databricks serverless compute instead of provisioning a cluster.
        When enabled, cluster-related parameters (worker_count, machine, spark_version,
        cluster_policy, existing_cluster_id) are ignored.
        See https://docs.databricks.com/api/workspace/jobs/submit for details.
    serverless_environment_version: str = "3"
        Serverless environment version. Each version comes with specific Python version
        and set of preinstalled packages. Default is "3".
        See https://docs.databricks.com/aws/release-notes/serverless/#serverless-environment-versions
        Only used when use_serverless=True.

    """  # noqa: E501

    storage_location: str
    worker_count: Union[int, DatabricksAutoscaleConfig] = 1
    machine: str = "i3.xlarge"
    spark_version: Optional[str] = None
    data_security_mode: Optional[str] = "SINGLE_USER"
    cluster_spec_overrides: Optional[dict] = None
    cluster_policy: Optional[str] = None
    existing_cluster_id: Optional[str] = None
    use_serverless: bool = False
    group_name: str = Field(default_factory=lambda: os.environ.get("DATABRICKS_GROUP", "default-group"))
    secret: Optional[DatabricksSecret] = None
    s3_credentials: Optional[dict] = None
    cloud_pickle_by_value: List[str] = Field(default_factory=list)
    cloud_pickle_infer_base_module: bool = True
    task_args: Optional[dict] = None
    python_libraries: list[Union[str, DatabricksPythonLibrary]] = Field(default_factory=list)
    timeout_seconds: int = 86400
    serverless_environment_version: str = "3"
    
    @model_validator(mode="after")
    def convert_string_libraries_to_objects(self):
        """Convert any string library names to DatabricksPythonLibrary objects for backwards compatibility.
        
        Supports formats:
        - "package_name" 
        - "package_name==version"
        """
        converted_libraries = []
        for lib in self.python_libraries:
            if isinstance(lib, str):
                # Parse string to extract name and version if specified
                if "==" in lib:
                    name, version = lib.split("==", 1)
                    converted_libraries.append(DatabricksPythonLibrary(name=name.strip(), version=version.strip()))
                else:
                    converted_libraries.append(DatabricksPythonLibrary(name=lib.strip()))
            else:
                converted_libraries.append(lib)
        self.python_libraries = converted_libraries
        return self
    
    @model_validator(mode="after")
    def validate_serverless_configuration(self):
        """Validate serverless vs cluster configuration parameters."""
        if self.use_serverless:
            # Validate storage location for serverless
            if not self.storage_location.startswith("/Volumes/"):
                logger.warning(
                    f"Serverless compute is enabled but storage_location is '{self.storage_location}'. "
                    "Serverless compute requires Unity Catalog Volumes (format: /Volumes/<catalog>/<schema>/<volume>/). "
                    "This may cause the job to fail."
                )
            
            # Warn if existing_cluster_id is set (will be ignored)
            if self.existing_cluster_id:
                logger.info(
                    "Serverless compute is enabled. The existing_cluster_id parameter will be ignored."
                )
            
            # Validate spark_version is not set for serverless
            if self.spark_version is not None:
                raise ValueError(
                    "spark_version must not be set when use_serverless=True. "
                    "Serverless compute runtime version is specified by serverless_environment_version."
                )
        else:
            # Validate spark_version is set for cluster compute
            if self.spark_version is None:
                raise ValueError(
                    "spark_version is required when use_serverless=False. "
                    "Please specify a Databricks runtime version (e.g., '17.3.x-scala2.13')."
                )
        
        return self
