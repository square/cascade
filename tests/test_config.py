from pyfakefs.fake_filesystem import FakeFilesystem
import pytest

from block_cascade.config import find_default_configuration
from block_cascade.executors.databricks.resource import (
    DatabricksAutoscaleConfig,
    DatabricksResource,
)
from block_cascade.executors.vertex.resource import (
    GcpEnvironmentConfig,
    GcpMachineConfig,
    GcpResource,
)

GCP_PROJECT = "test-project"
GCP_STORAGE_LOCATION = f"gs://{GCP_PROJECT}-cascade/"


@pytest.fixture(params=["cascade.yaml", "cascade.yml"])
def configuration_filename(request):
    return request.param


@pytest.fixture()
def storage_location():
    return GCP_STORAGE_LOCATION


@pytest.fixture()
def gcp_project():
    return GCP_PROJECT


@pytest.fixture()
def gcp_location():
    return "us-central1"


@pytest.fixture()
def gcp_service_account():
    return "test-project@test-project.iam.gserviceaccount.com"


@pytest.fixture()
def gcp_machine_config():
    return GcpMachineConfig(type="n1-standard-4", count=2)


@pytest.fixture
def gcp_environment(storage_location, gcp_project, gcp_location, gcp_service_account):
    return GcpEnvironmentConfig(
        storage_location=storage_location,
        project=gcp_project,
        service_account=gcp_service_account,
        region=gcp_location,
    )


@pytest.fixture()
def gcp_resource(gcp_environment, gcp_machine_config):
    return GcpResource(chief=gcp_machine_config, environment=gcp_environment)


@pytest.fixture()
def databricks_resource():
    return DatabricksResource(
        storage_location="s3://test-bucket/cascade",
        worker_count=DatabricksAutoscaleConfig(min_workers=5, max_workers=10),
        cloud_pickle_by_value=["a", "b"],
        spark_version="11.3.x-scala2.12",
    )


@pytest.fixture()
def test_job_name():
    return "hello-world"


def test_no_configuration():
    assert find_default_configuration() is None


def test_invalid_type_specified(fs: FakeFilesystem, configuration_filename: str):
    configuration = """
addition:
    type: AwsResource
"""
    fs.create_file(configuration_filename, contents=configuration)
    with pytest.raises(ValueError):
        find_default_configuration()


def test_gcp_resource(
    fs: FakeFilesystem,
    configuration_filename: str,
    gcp_resource: GcpResource,
    test_job_name: str,
):
    configuration = f"""
{test_job_name}:
    type: GcpResource
    chief:
        type: {gcp_resource.chief.type}
        count: {gcp_resource.chief.count}
    environment:
        storage_location: {gcp_resource.environment.storage_location}
        project: {gcp_resource.environment.project}
        service_account: {gcp_resource.environment.service_account}
        region: {gcp_resource.environment.region}
"""
    fs.create_file(configuration_filename, contents=configuration)
    assert gcp_resource == find_default_configuration()[test_job_name]


def test_databricks_resource(
    fs: FakeFilesystem,
    configuration_filename: str,
    databricks_resource: DatabricksResource,
    test_job_name: str,
):
    configuration = f"""
{test_job_name}:
    type: DatabricksResource
    storage_location: {databricks_resource.storage_location}
    worker_count:
        min_workers: {databricks_resource.worker_count.min_workers}
        max_workers: {databricks_resource.worker_count.max_workers}
    cloud_pickle_by_value:
        - a
        - b
    spark_version: {databricks_resource.spark_version}
"""
    fs.create_file(configuration_filename, contents=configuration)
    assert databricks_resource == find_default_configuration()[test_job_name]


def test_merged_resources(
    fs: FakeFilesystem,
    configuration_filename: str,
    test_job_name: str,
    gcp_resource: GcpResource,
):
    configuration = f"""
default:
    GcpResource:
        environment:
            storage_location: {gcp_resource.environment.storage_location}
            project: "ds-cash-dev"
            service_account: {gcp_resource.environment.service_account}
            region: {gcp_resource.environment.region}
{test_job_name}:
    type: GcpResource
    environment:
        project: {gcp_resource.environment.project}
    chief:
        type: {gcp_resource.chief.type}
        count: {gcp_resource.chief.count}
"""
    fs.create_file(configuration_filename, contents=configuration)
    assert gcp_resource == find_default_configuration()[test_job_name]
