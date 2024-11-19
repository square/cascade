import pytest
from unittest.mock import Mock, patch

from block_cascade.prefect.v2 import PrefectEnvironmentClient
from prefect.client.schemas.responses import DeploymentResponse
from prefect.context import FlowRunContext

@pytest.fixture
def mock_deployment_response():
    return Mock(spec=DeploymentResponse)

@pytest.fixture
def mock_flow_run_context(mocker, mock_deployment_response):
    mock_context = Mock(spec=FlowRunContext)
    mock_context.flow_run.deployment_id = "mock_deployment_id"
    mocker.patch("prefect.context.FlowRunContext.get", return_value=mock_context)
    mocker.patch("block_cascade.prefect.v2._fetch_deployment", return_value=mock_deployment_response)
    return mock_context

@pytest.fixture
def mock_infrastructure_block(mocker):
    infra_block = Mock()
    infra_block.data = {
        "image": "infra_image",
        "network": "infra_network",
        "gcp_credentials": {"project": "infra_project"},
        "region": "infra_region",
        "service_account": "infra_service_account"
    }
    mocker.patch("block_cascade.prefect.v2._fetch_block", return_value=infra_block)
    return infra_block

@pytest.fixture
def mock_job_variables():
    return {
        "image": "job_image",
        "network": "job_network",
        "credentials": {"project": "job_project"},
        "region": "job_region",
        "service_account_name": "job_service_account"
    }

def test_get_container_image(mock_flow_run_context, mock_infrastructure_block, mock_job_variables):
    client = PrefectEnvironmentClient()
    mock_flow_run_context.flow_run.job_variables = mock_job_variables

    assert client.get_container_image() == "job_image"

def test_get_network(mock_flow_run_context, mock_infrastructure_block, mock_job_variables):
    client = PrefectEnvironmentClient()
    mock_flow_run_context.flow_run.job_variables = mock_job_variables

    assert client.get_network() == "job_network"

def test_get_project(mock_flow_run_context, mock_infrastructure_block, mock_job_variables):
    client = PrefectEnvironmentClient()
    mock_flow_run_context.flow_run.job_variables = mock_job_variables

    assert client.get_project() == "job_project"

def test_get_region(mock_flow_run_context, mock_infrastructure_block, mock_job_variables):
    client = PrefectEnvironmentClient()
    mock_flow_run_context.flow_run.job_variables = mock_job_variables

    assert client.get_region() == "job_region"

def test_get_service_account(mock_flow_run_context, mock_infrastructure_block, mock_job_variables):
    client = PrefectEnvironmentClient()
    mock_flow_run_context.flow_run.job_variables = mock_job_variables

    assert client.get_service_account() == "job_service_account"

def test_fallback_to_infrastructure(mock_flow_run_context, mock_infrastructure_block):
    client = PrefectEnvironmentClient()
    mock_flow_run_context.flow_run.job_variables = None

    assert client.get_container_image() == "infra_image"
    assert client.get_network() == "infra_network"
    assert client.get_project() == "infra_project"
    assert client.get_region() == "infra_region"
    assert client.get_service_account() == "infra_service_account"