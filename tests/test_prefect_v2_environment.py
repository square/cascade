import pytest
from unittest.mock import Mock, patch

from prefect.client.schemas.responses import DeploymentResponse

from block_cascade.prefect.v2.environment import PrefectEnvironmentClient


@pytest.fixture(autouse=True)
def mock_infrastructure_block():
    infra_block = Mock()
    infra_block.data = {
        "image": "infra_image",
        "network": "infra_network",
        "gcp_credentials": {"project": "infra_project"},
        "region": "infra_region",
        "service_account": "infra_service_account"
    }
    with patch("block_cascade.prefect.v2.environment._fetch_block", return_value=infra_block):
        yield infra_block

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

@pytest.fixture(autouse=True)
def mock__fetch_deployment(mock_deployment_response):
    with patch("block_cascade.prefect.v2.environment._fetch_deployment", return_value=mock_deployment_response):
        yield

@pytest.fixture(autouse=True)
def mock_deployment_id():
    with patch("prefect.runtime.deployment.id", "mock_deployment_id"):
        yield

def test_get_container_image():
    client = PrefectEnvironmentClient()
    assert client.get_container_image() == "job_image"

def test_get_network():
    client = PrefectEnvironmentClient()
    assert client.get_network() == "job_network"

def test_get_project():
    client = PrefectEnvironmentClient()
    assert client.get_project() == "job_project"

def test_get_region():
    client = PrefectEnvironmentClient()
    assert client.get_region() == "job_region"

def test_get_service_account():
    client = PrefectEnvironmentClient()
    assert client.get_service_account() == "job_service_account"

def test_fallback_to_infrastructure(mock_deployment_response):
    client = PrefectEnvironmentClient()
    mock_deployment_response.job_variables = None

    assert client.get_container_image() == "infra_image"
    assert client.get_network() == "infra_network"
    assert client.get_project() == "infra_project"
    assert client.get_region() == "infra_region"
    assert client.get_service_account() == "infra_service_account"