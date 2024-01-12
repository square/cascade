import os
import pickle
from typing import Callable

from fsspec.implementations.local import LocalFileSystem

from block_cascade.executors.executor import Executor


class LocalExecutor(Executor):
    """Submits or runs tasks in a local process"""

    def __init__(self, func: Callable):
        super().__init__(func=func)
        self._fs = LocalFileSystem(auto_mkdir=True)

    @property
    def storage_location(self):
        """
        Returns the path to the local storage location for staging pickled functions
        and returning results. The directory is in the
        format /Users/<username>/cascade-storage/.
        """
        return f"{os.path.expanduser('~')}/cascade-storage/"

    def _start(self):
        """
        Starts a task to the local process by calling _execute.
        """
        return self._result()

    def _run(self):
        """
        Runs the task locally by calling _start(), called from Executor.run()
        """
        return self._start()

    def _result(self):
        """
        Executes a task by calling task.function() or tune(task) if
        task.tune is not None.
        """
        function = self.func
        result = function()
        with self.fs.open(self.output_filepath, "wb") as f:
            pickle.dump(result, f)
        return result
