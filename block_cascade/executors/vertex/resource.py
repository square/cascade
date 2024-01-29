from dataclasses import field
from typing import Iterable, Optional, Type, TypeVar

from pydantic import validator
from pydantic.dataclasses import dataclass

from block_cascade.executors.vertex.distributed.distributed_job import (
    DistributedJobBase,
)

T = TypeVar("T", bound="GcpEnvironmentConfig")


@dataclass(frozen=True)
class GcpAcceleratorConfig:
    """
    Description of a GPU accelerator to attach to a machine. Accelerator type and count
    must be compatabile with the machine type.
    See https://cloud.google.com/vertex-ai/docs/training/configure-compute#accelerators
    for valid machine_type, accelerator_type and count combinations.
    count: int = 1
    type: str = 'NVIDIA_TESLA_T4'
    """

    count: int = 1
    type: str = "NVIDIA_TESLA_T4"


@dataclass(frozen=True)
class NfsMountConfig:
    """
    Description of an NFS mount to attach to a machine.
    """

    server: str
    path: str
    mount_point: str


@dataclass(frozen=True)
class GcpMachineConfig:
    """
    Description of a VM type that will be provisioned for a job in GCP.
    GCPResources are composed of one or more machines.

    type: str = 'n1-standard-4'
        VertexAI machine type, default is n1-standard-4
        See https://cloud.google.com/vertex-ai/docs/training/configure-compute#machine-types
        https://cloud.google.com/compute/docs/machine-resource#recommendations_for_machine_types
    count: int = 1
        The number of machines of to provision in this node pool.
    min_replica_count: Optional[int] = None
        The minimum number of replicas to provision for this node pool. Only relevant for creating
        persistent resources
    max_replica_count: Optional[int] = None
        The maximum number of replicas to provision for this node pool. Only relevant for creating
        persistent resources
    accelerator: Optional[GcpAcceleratorConfig] = None
        Description of a GPU accelerator to attach to the machine.
        See https://cloud.google.com/vertex-ai/docs/training/configure-compute#accelerators
    disk_size_gb: Optional[int] = None
        Size of the boot disk in GB. If None, uses default size for machine type.
    nfs_mounts: Optional[Iterable[NfsMountConfig]] = None
        List of NFS mounts to attach to the machine. Specified via NfsMountConfig objects.
    """  # noqa: E501

    type: str = "n2-standard-4"
    count: int = 1
    min_replica_count: Optional[int] = None
    max_replica_count: Optional[int] = None
    accelerator: Optional[GcpAcceleratorConfig] = None
    disk_size_gb: Optional[int] = None
    nfs_mounts: Optional[Iterable[NfsMountConfig]] = None


@dataclass(config=dict(validate_assignment=True))
class GcpEnvironmentConfig:
    """
    Description of the specific GCP environment in which a job will run.
    A valid project and service account are required.

    project: str
        GCP Project used to launch job.
    storage_location: Optional[str]
        Path to the directory on GCS where files will be staged and output written
        default: f"gs://cascade-{project}/"
    service_account: Optional[str] = None
        The name of the service account that will be used for the job.
    region: Optional[str] = None
        The region in which to start the job.
    network: Optional[str] = None
        The name of the virtual network in which to start the job
    image: Optional[str] = None
        The full URL of the image or just the path component following
        the project name in the container registry URL.
    """

    project: str
    storage_location: Optional[str] = None
    service_account: Optional[str] = None
    region: Optional[str] = None
    network: Optional[str] = None
    image: Optional[str] = None

    def __post_init__(self):
        if self.storage_location is None:
            self.storage_location = f"gs://cascade-{self.project}/"

    @classmethod
    def with_shared_vpc(cls: T, **kwargs) -> Type[T]:
        return cls(
            network="projects/603986066384/global/networks/neteng-shared-vpc-prod",
            **kwargs,
        )

    @validator("image")
    def image_setter(cls, v, values):  # noqa: N805
        image = v
        # No image specified
        if image is None:
            return image
        # Full URL
        elif image and ("/" in image):
            return image
        # Just the image tag
        else:
            return f"us.gcr.io/{values['project']}/{image}"

    @property
    def is_complete(self):
        """
        Determines if the environment config has all required fields to launch a
        remote VertexAI job from cascade outside a Prefect context.
        Note that network is not required.
        """
        return all([self.project, self.service_account, self.region, self.image])


@dataclass
class GcpResource:
    """
    Description of a GCP computing resource and its environment
    A resource consists of a GCPEnvironmentConfig and one or more GCPMachineConfigs

    chief: GCPMachineConfig
        A config describing the chief worker pool.
    workers: Optional[GCPMachineConfig] = None
        The machine type of the worker machines.
    envrionment: Optional[GCPEnvironmentConfig] = None
        The GCP environment in which to run the job. If none, the environment will be
        inferred from the current Prefect context.
    persistent_resource_id: Optional[str] = None

    Set accelerators for GPU training by passing a `GcpAcceleratorConfig`
    to the chief or worker machine config object.
    """

    chief: GcpMachineConfig = field(default_factory=GcpMachineConfig)
    workers: GcpMachineConfig = None
    environment: Optional[GcpEnvironmentConfig] = None
    distributed_job: Optional[DistributedJobBase] = None
    persistent_resource_id: Optional[str] = None
