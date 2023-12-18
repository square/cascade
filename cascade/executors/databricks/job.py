""" Data model for task running on Databricks
"""
from dataclasses import dataclass
from importlib.metadata import version
from typing import Optional
from uuid import uuid4

from cascade.executors.databricks.resource import (
    DatabricksAutoscaleConfig,
    DatabricksResource,
)

ARTIFACTORY = "https://artifactory.global.square/artifactory/api/pypi/block-pypi/simple"


@dataclass(frozen=True)
class DatabricksJob:
    """A description of a job to run on Databricks

    Attributes
    ----------
    name
        The job display name on Vertex
    resource: DatabricksResource
        The execution cluster configuration on Databricks - see the docs for `DatabricksResource`.
    storage_path: str
        The full path to the directory for assets (input, output) on AWS (includes storage_key)
    storage_key: str
        A key suffixed to the storage location to ensure a unique path for each job
    idempotency_token: str
        A cache key for the job in Databricks; for checking the status of an active job
    cluster_policy_id: str
        Generated by default by looking up using team name
    existing_cluster_id: str
        The id of an existing cluster to use, if specified job_config is ignored.
    run_path: str
        Path to run.py bootstrapping script on AWS
    task_args: str
        Additional args to be passed to the task
    timeout_seconds: str
        The maximum time this job can run for; default is 24 hours
    """  # noqa: E501

    name: str
    resource: DatabricksResource
    storage_path: str
    storage_key: str
    run_path: str
    cluster_policy_id: str
    idempotency_token: str = uuid4().hex
    existing_cluster_id: Optional[str] = None
    timeout_seconds: str = 86400

    def create_payload(self):
        """"""
        task = self._task_spec()
        task.update({"existing_cluster_id": self.existing_cluster_id})
        task.update({"new_cluster": self._cluster_spec()})

        return {
            "tasks": [task],
            "run_name": self.name,
            "timeout_seconds": self.timeout_seconds,
            "idempotency_token": self.idempotency_token,
            "access_control_list": [
                {
                    "group_name": self.resource.group_name,
                    "permission_level": "CAN_MANAGE",
                },
            ],
        }

    def _task_spec(self):
        task_args = self.resource.task_args or {}

        if self.existing_cluster_id is None:
            if task_args.get("libraries") is None:
                task_args["libraries"] = []
            task_args["libraries"].extend(self._libraries())

        return {
            "task_key": f"{self.name[:32]}---{self.name[-32:]}",
            "description": "A function submitted from Cascade",
            "depends_on": [],
            "spark_python_task": {
                "python_file": self.run_path,
                "parameters": [self.storage_path, self.storage_key],
            },
            **task_args,
        }

    def _libraries(self):
        libraries_to_add = []

        python_libraries = self.resource.python_libraries or []

        if "cloudpickle" not in python_libraries:
            python_libraries.append("cloudpickle")
        if "prefect" not in python_libraries:
            python_libraries.append("prefect")

        for package_name in python_libraries:
            # if the resource construction specifies a version, use that,
            # otherwise use the version from the environment
            if "==" in package_name:
                package = package_name
            else:
                package = f"{package_name}=={version(package_name)}"
            libraries_to_add.append(
                {
                    "pypi": {
                        "package": package,
                        "repo": ARTIFACTORY,
                    }
                }
            )
        return libraries_to_add

    def _cluster_spec(self):
        """
        Creates a cluster spec for a Databricks job from the resource object
        passed to the DatabricksJobConfig object.
        """
        if self.existing_cluster_id:
            return None
        else:
            cluster_spec = {
                "spark_version": self.resource.spark_version,
                "node_type_id": self.resource.machine,
                "policy_id": self.cluster_policy_id,
                "data_security_mode": self.resource.data_security_mode,
                "single_user_name": None,
            }
            worker_count = self.resource.worker_count
            if (
                isinstance(worker_count, DatabricksAutoscaleConfig)
                or "DatabricksAutoscaleConfig" in type(worker_count).__name__
            ):
                workers = {
                    "autoscale": {
                        "min_workers": worker_count.min_workers,
                        "max_workers": worker_count.max_workers,
                    }
                }
            elif isinstance(worker_count, int):
                workers = {"num_workers": worker_count}
            else:
                raise TypeError(
                    f"Expected `worker_count` of type `DatabricksAutoscaleConfig` or "
                    f"`int` but received {type(worker_count)}"
                )

            cluster_spec.update(workers)
            if self.resource.cluster_spec_overrides is not None:
                cluster_spec.update(self.resource.cluster_spec_overrides)
            return cluster_spec
