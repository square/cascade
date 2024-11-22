from block_cascade import GcpEnvironmentConfig, GcpMachineConfig, GcpResource
from block_cascade import remote

GCP_PROJECT = "test-project"
GCP_STORAGE_LOCATION = f"gs://{GCP_PROJECT}-cascade/"


def test_remote_local_override():
    """Test the remote decorator with the local executor using the no_resource_on_local flag."""
    machine_config = GcpMachineConfig(type="n1-standard-4")
    gcp_resource = GcpResource(
        chief=machine_config,
        environment=GcpEnvironmentConfig(
            storage_location=GCP_STORAGE_LOCATION, project=GCP_PROJECT
        ),
    )

    @remote(resource=gcp_resource, remote_resource_on_local=False)
    def multiply(a: int, b: int) -> int:
        return a * b

    result = multiply(1, 2)

    assert result == 2
