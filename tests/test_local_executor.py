import os
from unittest.mock import MagicMock, patch

import cloudpickle
from fsspec.implementations.local import LocalFileSystem

from block.cascade.executors import LocalExecutor
from block.cascade.utils import wrapped_partial


def addition(a: int, b: int) -> int:
    """Adds two numbers together."""
    return a + b


# create a mock of addition and set its name to "addition"
mocked_addition = MagicMock(return_value=3)
mocked_addition.__name__ = "addition"

prepared_addition = wrapped_partial(addition, 1, 2)


def test_local_executor():
    """Test the local executor run method."""

    executor = LocalExecutor(func=prepared_addition)
    result = executor.run()
    assert result == 3


def test_run_twice():
    """Tests that if the executor is run twice
    the second run executes the function again and stores it in a unique file.
    """

    executor = LocalExecutor(func=mocked_addition)

    result1 = executor.run()
    result2 = executor.run()

    assert mocked_addition.call_count == 2
    assert result1 == result2


def test_new_executor():
    """
    Tests generating a new executor from an existing one.
    """
    mocked_addition.call_count = 0

    executor1 = LocalExecutor(func=mocked_addition)
    result1 = executor1.run()

    executor2 = executor1.with_()
    result2 = executor2.run()

    assert mocked_addition.call_count == 2
    assert executor1 != executor2
    assert result1 == result2


@patch("cascade.executors.executor.uuid4", return_value="12345")
def test_result(mock_uuid4):
    """
    Tests that a file containing a pickled function can be opened, the function run
    and the results written to a local filepath.
    """
    fs = LocalFileSystem(auto_mkdir=True)
    executor = LocalExecutor(func=mocked_addition)

    path_root = os.path.expanduser("~")

    # test that the staged_filepath was created correctly
    assert executor.staged_filepath == f"{path_root}/cascade-storage/12345/function.pkl"

    # stage the pickled function to the staged_filedpath
    with fs.open(executor.staged_filepath, "wb") as f:
        cloudpickle.dump(wrapped_partial, f)

    result = executor._result()
    assert result == 3
