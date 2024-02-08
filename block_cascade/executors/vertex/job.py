"""
Data model for running jobs on VertexAI
"""

from dataclasses import asdict, dataclass
import os
from typing import List, Mapping, Optional, Union

from slugify import slugify

from block_cascade.executors.vertex.resource import GcpMachineConfig, GcpResource
from block_cascade.executors.vertex.tune import (
    ParamCategorical,
    ParamDiscrete,
    ParamDouble,
    ParamInteger,
    Tune,
)

DISTRIBUTED_JOB_FILENAME = "distributed_job.pkl"


def _convert_to_gcp_compatible_label(val: str, is_key: bool = False) -> str:
    converted_val = slugify(val, max_length=63, regex_pattern=r"[^-a-z0-9_]+")
    if is_key and not converted_val:
        raise RuntimeError("Keys for GCP resources must be at least 1 " "character.")
    if is_key and not converted_val[0].isalpha():
        raise RuntimeError(
            "Keys for GCP resources must start with " "a lowercase letter."
        )
    return converted_val


@dataclass(frozen=True)
class VertexJob:
    """
    Data model to convert a GCP resource object into a valid payload
    for a VertexAI CustomJob request

    This class is intended to be used by the VertexExecutor, and is not intended for use
    by the end user of this library. It is exposed for testing purposes.

    This does not support the whole Vertex API, and assumes that you
    - are using custom containers (supplied via the GcpResource.image parameter)
    - are running a python function (or partial) inside that container (available at
      the staged_filepath)

    display_name: str
        The name of the job, used for logging and tracking
    resource: GcpResource
        The resource object describing the GCP environment and cluster configuration
        see block_cascade.executors.vertex.resource for details
    storage_path: str
        The path to the directory used to store the staged file and output
    tune: Optional[Tune] = None
        Whether to run a hyperparameter tuning job, and if so, how to configure it
    dashboard: Optional[bool] = False
        Whether to enable a hyperlink on the job page,
        in order to view the Dask dashboard
    web_console_access: Optional[bool] = False
        Whether to allow web console access to the job
    code_package: Optional[str] = None
        The GCS path to the users first party code that is added to sys.path
        at runtime to handle unpickling of a function that references
        first party code module.
    """

    display_name: str
    resource: GcpResource
    storage_path: str
    tune: Optional[Tune] = None
    dashboard: Optional[bool] = False
    web_console: Optional[bool] = False
    labels: Optional[Mapping[str, str]] = None
    code_package: Optional[str] = None

    @property
    def distributed_job_path(self):
        return os.path.join(self.storage_path, DISTRIBUTED_JOB_FILENAME)

    def create_payload(self):
        """Conversion from our resource data model to a CustomJob request on vertex

        https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.customJobs#CustomJob
        """
        gcp_compatible_labels = {
            _convert_to_gcp_compatible_label(
                key, is_key=True
            ): _convert_to_gcp_compatible_label(val)
            for key, val in (self.labels or {}).items()
        }
        if self.tune is None:
            return {
                "display_name": self.display_name,
                "job_spec": self._create_job_spec(),
                "labels": gcp_compatible_labels,
            }
        else:
            return {
                "display_name": self.display_name,
                "trial_job_spec": self._create_job_spec(),
                "max_trial_count": self.tune.trials,
                "parallel_trial_count": self.tune.parallel,
                "study_spec": self._create_study_spec(),
                "labels": gcp_compatible_labels,
            }

    def _create_job_spec(self):
        """
        Creates a CustomJobSpec from the resource object passed by the user.
        API docs at:
        https://cloud.google.com/vertex-ai/docs/reference/rest/v1/CustomJobSpec
        """
        job_spec = {
            "worker_pool_specs": self._create_cluster_spec(),
        }
        environment = self.resource.environment

        if environment.network is not None:
            job_spec["network"] = environment.network

        if environment.service_account is not None:
            job_spec["service_account"] = environment.service_account

        if self.dashboard is True:
            job_spec["enable_dashboard_access"] = True

        if self.web_console is True:
            job_spec["enable_web_access"] = True

        if self.resource.persistent_resource_id is not None:
            job_spec["persistent_resource_id"] = self.resource.persistent_resource_id

        return job_spec

    def _create_machine_pool_spec(self, machine_config: GcpMachineConfig):
        """
        Uses a machine config (descriping chief or worker pool) to a specification
        for a given machine pool in the Vertex API
        """
        node_pool_spec = {
            "replica_count": machine_config.count,
            "container_spec": self._create_container_spec(),
            "machine_spec": self._create_machine_spec(machine_config),
            "nfs_mounts": self._create_nfs_specs(machine_config),
        }

        if machine_config.disk_size_gb is not None:
            node_pool_spec["disk_spec"] = {
                "boot_disk_size_gb": machine_config.disk_size_gb
            }

        return node_pool_spec

    def _create_cluster_spec(self) -> List[dict]:
        """
        Creates a cluster spec from chief and (optional) worker specs
        https://cloud.google.com/vertex-ai/docs/reference/rest/v1/CustomJobSpec#WorkerPoolSpec
        """

        # First pool spec must have exactly one replica, its intended to be the "chief"
        # in single machine use cases this is the only spec
        cluster_spec = []
        if self.resource.chief.count != 1:
            raise ValueError(
                f"Chief pool must have exactly one replica, got {self.resource.chief.count}"  # noqa: E501
            )
        else:
            cluster_spec = []
            cluster_spec.append(self._create_machine_pool_spec(self.resource.chief))

        # worker pool specs are optional
        if self.resource.workers is not None:
            cluster_spec.append(self._create_machine_pool_spec(self.resource.workers))

        return cluster_spec

    def _create_container_spec(self):
        """
        The image to use to create a container and the entrypoint for
        that container
        """

        executor_module_path = "block_cascade.executors.vertex.run"
        distributed_job = "False"
        if self.resource.distributed_job is not None:
            distributed_job = "True"

        command = [
            "python",
            "-m",
            executor_module_path,
            self.storage_path,
            distributed_job,
            self.code_package or "",
        ]

        return {
            "image_uri": self.resource.environment.image,
            "command": command,
            "args": [],
        }

    @staticmethod
    def _create_machine_spec(machine_config: GcpMachineConfig):
        """
        Adds chief_machine_spec information to the job_spec
        """
        machine_spec = {"machine_type": machine_config.type}
        if machine_config.accelerator is not None:
            machine_spec["accelerator_type"] = machine_config.accelerator.type
            machine_spec["accelerator_count"] = machine_config.accelerator.count
        return machine_spec

    @staticmethod
    def _create_nfs_specs(machine_config: GcpMachineConfig) -> List[dict]:
        """
        Produce a list of NFS mount specs from a machine config
        """
        nfs_specs = []
        if machine_config.nfs_mounts is not None:
            for nfs_mount in machine_config.nfs_mounts:
                nfs_specs.append(asdict(nfs_mount))
        return nfs_specs

    @staticmethod
    def _create_disk_spec(machine_config: GcpMachineConfig) -> dict:
        """
        Produce a disk spec from a machine config
        """
        if machine_config.disk_size_gb is not None:
            return {"boot_disk_size_gb": machine_config.disk_size_gb}
        return None

    # tuning spec creation
    def _create_study_spec(self) -> dict:
        """
        Create a study specification dictionary from the tuning metrics, goals,
        and parameters
        """
        study_spec = {
            "metrics": [{"metric_id": self.tune.metric, "goal": self.tune.goal}],
            "parameters": [self._create_parameter_spec(p) for p in self.tune.params],
        }
        if self.tune.algorithm is not None:
            study_spec["algorithm"] = self.tune.algorithm
        return study_spec

    def _create_parameter_spec(
        self, param: Union[ParamDiscrete, ParamCategorical, ParamInteger, ParamDouble]
    ) -> dict:
        """
        Create a parameter specification dictionary with
        sub-specifications for each type of value
        """
        s = {"parameter_id": param.name}

        if hasattr(param, "scale") and param.scale is not None:
            s["scale_type"] = param.scale.name

        if isinstance(param, ParamDouble):
            s["double_value_spec"] = {"min_value": param.min, "max_value": param.max}
        elif isinstance(param, ParamInteger):
            s["integer_value_spec"] = {"min_value": param.min, "max_value": param.max}
        elif isinstance(param, ParamCategorical):
            s["categorical_value_spec"] = {"values": param.values}
        elif isinstance(param, ParamDiscrete):
            s["discrete_value_spec"] = {"values": param.values}
        return s
