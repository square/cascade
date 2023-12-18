import os
from unittest import TestCase
from unittest.mock import PropertyMock, patch

import cloudpickle
from fsspec.implementations.local import LocalFileSystem
from google.cloud.aiplatform.compat.types import job_state_v1 as job_state

from cascade import GcpMachineConfig, GcpResource
from cascade.executors.vertex.executor import (
    Status,
    VertexCancelledError,
    VertexExecutor,
)
from cascade.executors.vertex.job import VertexJob
from cascade.utils import wrapped_partial
from tests.resource_fixtures import gcp_environment


# create a basic function
def add(a: int, b: int) -> int:
    return a + b


# status of job as global variables
CANCELLED_STATUS = Status(job_state.JobState.JOB_STATE_CANCELLED, "test job cancelled")
STAGE_METHOD = "cascade.executors.vertex.executor.Executor._stage"
STATUS_METHOD = "cascade.executors.vertex.executor.VertexExecutor._get_status"
START_METHOD = "cascade.executors.vertex.executor.VertexExecutor._start"
VERTEX_PROPERTY = "cascade.executors.vertex.executor.VertexExecutor.vertex"
STORAGE_PATH = "cascade.executors.vertex.executor.VertexExecutor.storage_path"

# Create a GCP resource
machine_config = GcpMachineConfig("n1-standard-1")
environment_config = gcp_environment

gcp_resource = GcpResource(
    machine_config,
    environment=environment_config,
)


class TestVertexExecutor(TestCase):
    def setUp(self):
        self.vertex_executor = VertexExecutor(resource=gcp_resource, func=add)

    @patch(STAGE_METHOD)
    @patch(START_METHOD, return_value="test_job")
    @patch(STATUS_METHOD, return_value=CANCELLED_STATUS)
    @patch(VERTEX_PROPERTY, return_value="dummy_api")
    def test_run(self, stage_mock, start_mock, status_mock, _):
        """
        Tests that the VertexExecutor.run() method calls the correct private methods
        """
        # swap the fs for a local fs
        self.vertex_executor.fs = LocalFileSystem()
        self.vertex_executor.storage_location = (
            f"{os.path.expanduser('~')}/cascade-storage/"
        )

        with self.assertRaises(VertexCancelledError):
            self.vertex_executor.run()

        assert stage_mock.called_once()
        assert start_mock.called_once()
        assert status_mock.called_once()

    def test_create_job(self):
        """
        Tests that a can be VertexJob is from a VertexExecutor.
        """

        test_job = self.vertex_executor.create_job()
        assert isinstance(test_job, VertexJob)

        custom_job = test_job.create_payload()
        assert isinstance(custom_job, dict)

    @patch(STORAGE_PATH, new_callable=PropertyMock, return_value="/tmp/test")
    def test_stage(self, mock_storage_path):
        """
        Tests that the VertexExecutor._stage() correctly stages a function
        """

        executor = VertexExecutor(
            resource=gcp_resource,
            func=wrapped_partial(add, 1, 2),
        )

        # use the local filesystem for test
        executor.fs = LocalFileSystem(auto_mkdir=True)

        executor._stage()

        with executor.fs.open(executor.staged_filepath, "rb") as f:
            func = cloudpickle.load(f)

        assert func() == 3
