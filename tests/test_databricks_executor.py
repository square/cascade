from functools import partial
from unittest.mock import patch

from block_cascade import DatabricksResource
from block_cascade.executors import DatabricksExecutor
from block_cascade.executors.databricks.job import DatabricksJob
from block_cascade.utils import wrapped_partial

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
    spark_version="11.3.x-scala2.12",
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


def test_serverless_job_creation():
    """Test that a serverless job is created correctly without cluster configuration."""
    serverless_resource = DatabricksResource(
        storage_location="s3://test-bucket/cascade",
        group_name=DATABRICKS_GROUP,
        use_serverless=True,
        python_libraries=["pandas", "numpy"],
    )
    
    executor = DatabricksExecutor(
        func=addition_packed,
        resource=serverless_resource,
    )
    
    # Should not require cluster policy lookup for serverless
    databricks_job = executor.create_job()
    assert isinstance(databricks_job, DatabricksJob)
    assert databricks_job.cluster_policy_id is None
    
    # Verify the payload structure
    payload = databricks_job.create_payload()
    task = payload["tasks"][0]
    
    # Task should reference environment by key
    assert "environment_key" in task
    assert task["environment_key"] == "default"
    
    # Task should NOT have cluster configuration
    assert task.get("existing_cluster_id") is None
    assert task.get("new_cluster") is None
    
    # Libraries should NOT be at task level for serverless
    assert "libraries" not in task or len(task.get("libraries", [])) == 0
    
    # Environments should be defined at job level
    assert "environments" in payload
    assert len(payload["environments"]) == 1
    
    env = payload["environments"][0]
    assert env["environment_key"] == "default"
    assert "spec" in env
    assert "dependencies" in env["spec"]
    assert "environment_version" in env["spec"]
    assert env["spec"]["environment_version"] == "3"
    
    # Verify dependencies include required libraries
    dependencies = env["spec"]["dependencies"]
    assert isinstance(dependencies, list)
    assert any("cloudpickle" in dep for dep in dependencies)
    assert any("prefect" in dep for dep in dependencies)
    assert any("pandas" in dep for dep in dependencies)
    assert any("numpy" in dep for dep in dependencies)