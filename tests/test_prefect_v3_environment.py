import pytest
from unittest.mock import Mock, patch

from prefect.client.schemas.responses import DeploymentResponse

@pytest.fixture
def prefect_environment_client(mock_deployment_response):
    with patch("block_cascade.prefect.PREFECT_VERSION", 3), \
        patch("block_cascade.prefect.v3.environment.runtime.deployment.id", "mock-deployment-id"), \
        patch("block_cascade.prefect.v3.environment._fetch_deployment", return_value=mock_deployment_response):
        from block_cascade.prefect.v3.environment import PrefectEnvironmentClient
        client = PrefectEnvironmentClient()
        yield client


@pytest.fixture
def mock_job_variables():
    return {
        "image": "job_image",
        "network": "job_network",
        "credentials": {"project": "job_project"},
        "region": "job_region",
        "service_account_name": "job_service_account"
    }

@pytest.fixture
def mock_deployment_response(mock_job_variables):
    mock_deployment = Mock(spec=DeploymentResponse)
    mock_deployment.job_variables = mock_job_variables
    mock_deployment.infrastructure_document_id = "mock_infrastructure_id"
    return mock_deployment

def test_get_container_image(prefect_environment_client):
    assert prefect_environment_client.get_container_image() == "job_image"

def test_get_network(prefect_environment_client):
    assert prefect_environment_client.get_network() == "job_network"

def test_get_project(prefect_environment_client):
    assert prefect_environment_client.get_project() == "job_project"

def test_get_region(prefect_environment_client):
    assert prefect_environment_client.get_region() == "job_region"

def test_get_service_account(prefect_environment_client):
    assert prefect_environment_client.get_service_account() == "job_service_account"
