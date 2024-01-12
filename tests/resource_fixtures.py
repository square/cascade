from block_cascade.executors.vertex.resource import (
    GcpEnvironmentConfig,
    GcpMachineConfig,
    GcpResource,
)

GCP_PROJECT = "test-project"
TEST_BUCKET = GCP_PROJECT
REGION = "us-west1"

chief_machine = GcpMachineConfig("n1-standard-4", 1)
gcp_environment = GcpEnvironmentConfig(
    project=GCP_PROJECT,
    storage_location="gs://bucket/path/to/file",
    region="us-west1",
    service_account=f"{GCP_PROJECT}@{GCP_PROJECT}.iam.gserviceaccount.com",
    image="cascade",
)
gcp_resource = GcpResource(
    chief=chief_machine,
    environment=gcp_environment,
)
