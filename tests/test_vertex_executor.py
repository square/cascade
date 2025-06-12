import os
import pytest
from unittest.mock import PropertyMock, patch

import cloudpickle
from fsspec.implementations.local import LocalFileSystem
from google.cloud.aiplatform_v1beta1.types import job_state

from block_cascade import GcpMachineConfig, GcpResource
from block_cascade.executors.vertex.executor import (
    Status,
    VertexCancelledError,
    VertexExecutor,
)
from block_cascade.executors.vertex.job import VertexJob
from block_cascade.utils import wrapped_partial
from tests.resource_fixtures import gcp_environment


# create a basic function
def add(a: int, b: int) -> int:
    return a + b


# status of job as global variables
CANCELLED_STATUS = Status(job_state.JobState.JOB_STATE_CANCELLED, "test job cancelled")
STAGE_METHOD = "block_cascade.executors.vertex.executor.Executor._stage"
STATUS_METHOD = "block_cascade.executors.vertex.executor.VertexExecutor._get_status"
START_METHOD = "block_cascade.executors.vertex.executor.VertexExecutor._start"
VERTEX_PROPERTY = "block_cascade.executors.vertex.executor.VertexExecutor.vertex"
STORAGE_PATH = "block_cascade.executors.vertex.executor.VertexExecutor.storage_path"
FILESYSTEM = "block_cascade.executors.vertex.executor.gcsfs.GCSFileSystem"

# Create a GCP resource
machine_config = GcpMachineConfig(type="n1-standard-1")
environment_config = gcp_environment

gcp_resource = GcpResource(
    chief=machine_config,
    environment=environment_config,
)

@pytest.fixture
def vertex_executor_fixture():
    with patch(STAGE_METHOD) as stage_mock, \
         patch(START_METHOD, return_value="test_job") as start_mock, \
         patch(STATUS_METHOD, return_value=CANCELLED_STATUS) as status_mock, \
         patch(VERTEX_PROPERTY, return_value="dummy_api"), \
         patch(FILESYSTEM, LocalFileSystem):
        
        vertex_executor = VertexExecutor(
            resource=gcp_resource,
            func=wrapped_partial(add, 1, 2),
        )
        vertex_executor.storage_location = (
            f"{os.path.expanduser('~')}/cascade-storage"
        )

        stage_mock.reset_mock()
        start_mock.reset_mock()
        status_mock.reset_mock()

        yield vertex_executor, stage_mock, start_mock, status_mock

def test_run(vertex_executor_fixture):
    """
    Tests that the VertexExecutor.run() method calls the correct private methods
    """
    # swap the fs for a local fs
    vertex_executor, stage_mock, start_mock, status_mock = vertex_executor_fixture

    with pytest.raises(VertexCancelledError):
        vertex_executor.run()

    start_mock.assert_called_once()
    status_mock.assert_called_once()
    stage_mock.assert_called_once()

def test_create_job(vertex_executor_fixture):
    """
    Tests that a VertexJob can be created from a VertexExecutor.
    """
    vertex_executor = vertex_executor_fixture[0]
    test_job = vertex_executor.create_job()
    assert isinstance(test_job, VertexJob)

    custom_job = test_job.create_payload()
    assert isinstance(custom_job, dict)

def test_stage(tmp_path):
    """
    Tests that the VertexExecutor._stage() correctly stages a function
    """
    with patch(VERTEX_PROPERTY, return_value="dummy_api"), \
         patch(FILESYSTEM, LocalFileSystem):
        
        executor = VertexExecutor(
            resource=gcp_resource,
            func=wrapped_partial(add, 1, 2),
        )
        executor._fs = LocalFileSystem(auto_mkdir=True)
        executor.storage_location = str(tmp_path)

        executor._stage()

        with executor.fs.open(executor.staged_filepath, "rb") as f:
            func = cloudpickle.load(f)

        assert func() == 3

