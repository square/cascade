from copy import copy

from cascade.executors.vertex.resource import GcpEnvironmentConfig
from tests.resource_fixtures import (
    gcp_environment,
    gcp_resource,
    TEST_BUCKET,
    GCP_PROJECT,
)

BASIC_MACHINE = "n1-standard-4"
STORAGE_LOCATION = "gs://bucket/path/to/file"


def test_gcp_resource():
    """Tests that a GCP resource can be instantiated from valid parameters."""

    # Test that the chief node is correctly configured
    assert gcp_resource.chief.type == BASIC_MACHINE
    assert gcp_resource.chief.count == 1

    # Test that the worker node is absent as expected
    assert gcp_resource.workers is None

    # Test that the environment is correctly configured
    assert gcp_resource.environment.project == "test-project"
    assert gcp_resource.environment.region == "us-west1"
    assert (
        gcp_resource.environment.service_account
        == "test-project@test-project.iam.gserviceaccount.com"
    )
    assert gcp_resource.environment.image == f"us.gcr.io/{TEST_BUCKET}/cascade"


def test_gcp_environment():
    """
    Tests that a GCP environment can be instantiated from valid parameters.
    And that its "is_complete" method works as expected.
    """
    gcp_environment_complete = gcp_environment

    assert gcp_environment_complete.is_complete is True

    gcp_environment_incomplete = copy(gcp_environment_complete)
    gcp_environment_incomplete.image = None

    assert gcp_environment_incomplete.is_complete is False


def test_gcp_resource_image():
    """Tests that an image can be overriden on a GcpEnvironment  object."""
    environment_config = gcp_environment

    assert environment_config.image == f"us.gcr.io/{TEST_BUCKET}/cascade"

    # update the image value to use a different GCR path
    environment_config.image = f"gcr.io/{TEST_BUCKET}/rapids"
    assert environment_config.image == f"gcr.io/{TEST_BUCKET}/rapids"

    # update the image value to a different name with a tag
    environment_config.image = "cascade:latest"
    assert environment_config.image == f"us.gcr.io/{TEST_BUCKET}/cascade:latest"

    # create an object with no image value
    environment_config_no_image = GcpEnvironmentConfig(
        project=GCP_PROJECT, storage_location=STORAGE_LOCATION
    )
    assert environment_config_no_image.image is None
