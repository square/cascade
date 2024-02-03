import logging
import os
from unittest.mock import Mock

import pytest

from block_cascade import GcpEnvironmentConfig, GcpMachineConfig, GcpResource
from block_cascade import remote
from block_cascade.utils import PREFECT_VERSION

if PREFECT_VERSION == 2:
    from prefect.context import FlowRunContext, TaskRunContext

GCP_PROJECT = "test-project"


@pytest.fixture(autouse=True)
def patch_prefect_apis(mocker):
    if PREFECT_VERSION == 2:
        mocker.patch(
            "block_cascade.prefect.v2.get_run_logger",
            return_value=Mock(spec=logging.Logger),
        )
        mocker.patch(
            "block_cascade.prefect.v2.FlowRunContext",
            return_value=Mock(spec=FlowRunContext),
        )
        mocker.patch(
            "block_cascade.prefect.v2.TaskRunContext",
            return_value=Mock(spec=TaskRunContext),
        )
        mocker.patch(
            "block_cascade.prefect.v2.get_current_deployment", return_value=None
        )


def test_remote():
    """Test the remote decorator with the local executor."""

    @remote
    def addition(a: int, b: int) -> int:
        return a + b

    result = addition(1, 2)

    assert result == 3


def test_remote_no_sugar():
    """Test using the decorator with syntactic sugar."""

    def addition(a: int, b: int) -> int:
        return a + b

    addition_remote = remote(func=addition)
    result = addition_remote(1, 2)
    assert result == 3


def test_exception_when_environment_cannot_be_inferred():
    machine_config = GcpMachineConfig(type="n1-standard-4")
    remote_resource = GcpResource(
        chief=machine_config, environment=GcpEnvironmentConfig(project=GCP_PROJECT)
    )

    @remote
    def addition(a: int, b: int) -> int:
        return a + b

    with pytest.raises(RuntimeError):
        addition(
            1,
            2,
            remote_resource=remote_resource,
        )
