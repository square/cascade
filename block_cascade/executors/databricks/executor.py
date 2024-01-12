from types import ModuleType
from typing import Callable, Iterable, Optional

from block_cascade.executors.executor import Executor

try:
    import cloudpickle
except ImportError:
    import pickle as cloudpickle  # Databricks renames cloudpickle to pickle in Runtimes 11 +  # noqa: E501

import importlib
import os
import sys
import threading
import time
import s3fs
from dataclasses import dataclass
from slugify import slugify

from databricks_cli.cluster_policies.api import ClusterPolicyApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient

from block_cascade.executors.databricks.resource import DatabricksSecret
from block_cascade.executors.databricks.job import DatabricksJob
from block_cascade.executors.databricks.resource import DatabricksResource
from block_cascade.prefect import get_prefect_logger

if sys.version_info.major >= 3 and sys.version_info.minor >= 9:
    from importlib.resources import files
else:  # python <3.9
    from importlib_resources import files

lock = threading.Lock()

# must specify API version=2.1 or runs submitted from Vertex are not viewable in
# Databricks UI
DATABRICKS_API_VERSION = "2.1"


class DatabricksError(Exception):
    pass


class DatabricksCancelledError(Exception):
    pass


@dataclass
class Status:
    """
    https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsrunlifecyclestate
    https://docs.databricks.com/dev-tools/api/2.0/jobs.html#runresultstate
    """

    status: dict

    def __post_init__(self):
        self.result_state = self.status["state"].get("result_state", "")
        self.life_cycle_state = self.status["state"]["life_cycle_state"]

    def is_executing(self):
        return self.life_cycle_state in {"PENDING", "RUNNING"}

    def is_cancelled(self):
        return self.result_state == "CANCELED"

    def is_succesful(self):
        return self.result_state == "SUCCESS"


class DatabricksExecutor(Executor):
    def __init__(
        self,
        func: Callable,
        resource: DatabricksResource,
        name: Optional[str] = None,
    ):
        """Executor to submit tasks to run as databricks jobs

        Parameters
        ----------
        func : Callable
            Function to run
        resource : DatabricksResource, optional
            Databricks resource, describing the cluster to run the job on
        job_name: str, optional
            An optional name for the job, by default this is None and inferred from
            func.__name__
        """
        super().__init__(func=func)
        self.resource = resource
        self.name = name
        self.active_job = None
        self._fs = None
        self._databricks_secret = resource.secret
        self._storage_location = resource.storage_location

        # extract params from resource
        self.group_name = self.resource.group_name
        if self.resource.cluster_policy is None:
            self.cluster_policy = self.group_name + "_default"
        else:
            self.cluster_policy = self.resource.cluster_policy

        self.logger = get_prefect_logger(__name__)

    @property
    def databricks_secret(self):
        """
        DatabricksSecret object containing token and host
        Check if the user passed a DatabricksSecret object or if
        DATABRICKS_HOST and DATABRICKS_TOKEN are set as ENV VARS
        if neither are set, raise a ValueError
        """
        if self._databricks_secret is not None:
            return self._databricks_secret
        elif os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
            return DatabricksSecret(
                host=os.environ["DATABRICKS_HOST"],
                token=os.environ["DATABRICKS_TOKEN"],
            )
        else:
            raise ValueError(
                """Cannot locate Databricks secret. Databricks secret must
            be set in DatabricksResource or as environment variables
            DATABRICKS_HOST and DATABRICKS_TOKEN"""
            )

    @property
    def fs(self):
        """
        # refresh this every time to get new (1 hour validity) creds
        # boto3 client creation is not threadsafe. if multiple DaskExecutor threads
        # try to call STS to get token at same time, an error is rasied:
        # `KeyError: 'endpoint_resolver`
        # wrap in retries:
        """
        wait = 1
        n_retries = 0
        while n_retries <= 6:
            try:
                if self.resource.s3_credentials is None:
                    self._fs = s3fs.S3FileSystem()
                else:
                    self._fs = s3fs.S3FileSystem(**self.resource.s3_credentials)
                break
            except KeyError:
                self.logger.info(f"Waiting {wait} seconds to retry STS")
                n_retries += 1
                time.sleep(wait)
                wait *= 1.5
        if self._fs is None:
            raise RuntimeError(
                "Failed to initialize S3 filesystem; job pickle cannot be staged."
            )
        return self._fs

    @fs.setter
    def fs(self, fs):
        self._fs = fs

    @property
    def cloudpickle_by_value(self) -> Iterable[ModuleType]:
        """
        A list of modules to pickle by value rather than by reference
        This list is defined by the user in the resource object and
        has signature List[str]

        If a module has not already present in sys.modules,
        it will be imported

        If a module is not found in the current Python environment,
        raises a Runtime error

        Returns
        -------
        Iterable[str]
            Set of modules to pickle by value
        """
        modules_to_pickle = set()
        for module in self.resource.cloud_pickle_by_value or []:
            try:
                modules_to_pickle.add(importlib.import_module(module))
            except ModuleNotFoundError:
                raise RuntimeError(
                    f"Unable to pickle {module} due to module not being "
                    "found in current Python environment."
                )
            except ImportError:
                raise RuntimeError(f"Unable to pickle {module} due to import error.")
        return modules_to_pickle

    @property
    def api_client(self):
        """
        TODO: We may be able to cache this/not recreate it every time;
        initially we're copying the previous AIP approach
        """
        api_client = ApiClient(
            host=self.databricks_secret.host, token=self.databricks_secret.token
        )
        return api_client

    @property
    def runs_api(self):
        return RunsApi(self.api_client)

    def get_cluster_policies(self):
        client = ClusterPolicyApi(self.api_client)
        policies = client.list_cluster_policies()
        return policies

    def get_cluster_policy_id_from_policy_name(self, cluster_policy_name: str) -> str:
        policies = self.get_cluster_policies()
        for i in policies["policies"]:
            if i["name"] == cluster_policy_name:
                return i["policy_id"]
        raise ValueError("No policy with provided name found")

    @property
    def run_path(self):
        return os.path.join(self.storage_path, "run.py")

    def create_job(self):
        """
        Create a DatabricksJob object
        """
        try:
            self.name = self.name or self.func.__name__
        except AttributeError:
            self.name = self.name or "unnamed"

        return DatabricksJob(
            name=slugify(self.name),
            resource=self.resource,
            storage_path=self.storage_path,
            storage_key=self.storage_key,
            existing_cluster_id=self.resource.existing_cluster_id,
            cluster_policy_id=self.get_cluster_policy_id_from_policy_name(
                self.cluster_policy
            ),
            run_path=self.run_path,
        )

    def _run(self):
        """
        Create the payload, submit it to the API, and monitor its status while it
        is executing
        """

        self._stage()
        self._start()

        while self._status().is_executing():
            time.sleep(30)

        if self._status().is_cancelled():
            raise DatabricksCancelledError(
                f"Job {self.name} was cancelled: {self._status().status}"
            )

        if not self._status().is_succesful():
            raise DatabricksError(f"Job {self.name} failed: {self._status().status}")

        return self._result()

    def _upload_run_script(self):
        """Create a script from cascade.executors.databricks.run.py and upload it
        to s3
        """
        run_script = (
            files("block_cascade.executors.databricks")
            .joinpath("run.py")
            .resolve()
            .as_posix()
        )
        self.fs.upload(run_script, self.run_path)

    def _stage(self):
        """
        Overwrite the base _stage method to additionally stage
        block_cascade.executors.databricks.run.py and register pickle by value dependencies
        and then unregister them
        """
        self._upload_run_script()

        with lock:
            for dep in self.cloudpickle_by_value:
                cloudpickle.register_pickle_by_value(dep)

            with self.fs.open(self.staged_filepath, "wb") as f:
                cloudpickle.dump(self.func, f)

            for dep in self.cloudpickle_by_value:
                cloudpickle.unregister_pickle_by_value(dep)

    def _start(self):
        """Create a job, use it to create a payload, and submit it to the API"""

        # get the databricks client and submit a job to the API
        client = self.runs_api
        job = self.create_job()
        databricks_payload = job.create_payload()
        self.logger.info(f"Databricks job payload: {databricks_payload}")

        self.active_job = client.submit_run(
            databricks_payload, version=DATABRICKS_API_VERSION
        )

        self.logger.info(f"Created Databricks job: {self.active_job}")
        url = client.get_run(**self.active_job)["run_page_url"]

        self.logger.info(f"Databricks job running: {url}")

    def _status(self, raw=False):
        runs_client = self.runs_api
        status = runs_client.get_run(**self.active_job)
        if raw:
            return status
        return Status(status)

    def list_runtime_versions(self):
        from databricks_cli.clusters.api import ClusterApi

        clusterapi = ClusterApi(self.api_client)
        versions = clusterapi.spark_versions()
        return sorted(versions["versions"], key=lambda x: x["key"])
