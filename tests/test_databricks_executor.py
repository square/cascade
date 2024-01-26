from functools import partial
from unittest.mock import patch

from block_cascade import DatabricksResource
from block_cascade.executors import DatabricksExecutor
from block_cascade.executors.databricks.job import DatabricksJob
from block_cascade.utils import wrapped_partial

databricks_resource = DatabricksResource()

# Mocks paths
MOCK_CLUSTER_POLICY = (
    "block_cascade.executors.DatabricksExecutor.get_cluster_policy_id_from_policy_name"
)
MOCK__RUN = "cascade.executors.DatabricksExecutor._run"
MOCK_FILESYSTEM = "cascade.executors.DatabricksExecutor.fs"
MOCK_STORAGE_PATH = "cascade.executors.DatabricksExecutor.storage_path"

DATABRICKS_GROUP = "cascade"

databricks_resource = DatabricksResource(
    storage_location="s3://test-bucket/cascade",
    group_name=DATABRICKS_GROUP,
)


def addition(a: int, b: int) -> int:
    return a + b


addition_packed = wrapped_partial(addition, 1, 2)


def test_create_executor():
    """Test that a DatabricksExecutor can be created."""
    _ = DatabricksExecutor(
        func=addition_packed,
        resource=databricks_resource,
    )


@patch(MOCK_CLUSTER_POLICY, return_value="12345")
def test_create_job(mock_cluster_policy):
    """Test that the creat_job method returns a valid DatabricksJob object."""
    executor = DatabricksExecutor(
        func=addition_packed,
        resource=databricks_resource,
    )
    databricks_job = executor.create_job()
    assert isinstance(databricks_job, DatabricksJob)


@patch(MOCK_CLUSTER_POLICY, return_value="12345")
def test_infer_name(mock_cluster_policy):
    """Test that if no name is provided, the name is inferred correctly."""
    executor = DatabricksExecutor(
        func=addition_packed,
        resource=databricks_resource,
    )
    assert executor.name is None
    _ = executor.create_job()
    assert executor.name == "addition"

    partial_func = partial(addition, 1, 2)

    executor_partialfunc = DatabricksExecutor(
        func=partial_func,
        resource=databricks_resource,
    )

    assert executor_partialfunc.name is None
    _ = executor_partialfunc.create_job()
    assert executor_partialfunc.name == "unnamed"
