import abc
from copy import copy
import os
from uuid import uuid4

import cloudpickle
from fsspec.implementations.local import LocalFileSystem

INPUT_FILENAME = "function.pkl"
OUTPUT_FILENAME = "output.pkl"


class Executor(abc.ABC):
    """
    Base class for an executor, which is the interface to run a function on some arbitraty resource.
    The main entry point for an executor is to call the run() method.
    A typical lifecyele for an executor is
    1. Create an instance of the executor
    2. Call the run() method, which calls the _run() method, which in turn calls _stage() and _start()
    and returns the result of the function by loading it from the output filepath and unpickling it.
    """  # noqa: E501

    def __init__(self, func):
        """
        Args:
            func: Callable
                The function to be run by the executor.
                Note:
                 * this function must be picklable
                 * expect no arguments
                 * have a __name__ attribute
            use block_cascade.utils.wrapped_partial to prepare a function for execution
        """
        self.func = func
        self._fs = LocalFileSystem(auto_mkdir=True)
        self._storage_location = None
        self.name = None
        self.storage_key = str(uuid4())

    @property
    def fs(self):
        return self._fs

    @fs.setter
    def fs(self, new_fs):
        self._fs = new_fs

    @property
    def input_filename(self):
        return INPUT_FILENAME

    @property
    def output_filename(self):
        return OUTPUT_FILENAME

    @property
    def func_name(self):
        """The name of the function being executed."""
        try:
            name = self.name or self.func.__name__
        except AttributeError:
            name = self.name or "unnamed"
        return name

    @property
    def storage_location(self):
        return self._storage_location

    @storage_location.setter
    def storage_location(self, storage_location: str):
        self._storage_location = storage_location

    @property
    def storage_path(self):
        return os.path.join(self.storage_location, self.storage_key)

    @property
    def output_filepath(self):
        """The path to the output file for the pickled result from the function."""
        return os.path.join(self.storage_path, self.output_filename)

    @property
    def staged_filepath(self):
        return os.path.join(self.storage_path, self.input_filename)

    def _stage(self):
        """
        Stage the job to run but do not run it
        """

        with self.fs.open(self.staged_filepath, "wb") as f:
            cloudpickle.dump(self.func, f)

    @abc.abstractmethod
    def _start(self):
        """
        Start the function on the resource.
        """
        raise NotImplementedError

    def run(self):
        """
        Run the child executor's _run function and return the result
        """
        return self._run()

    @abc.abstractmethod
    def _run(self):
        """Run the function and save the output to self.output(name)"""
        raise NotImplementedError

    def _result(self):
        try:
            with self.fs.open(self.output_filepath, "rb") as f:
                result = cloudpickle.load(f)
            self.fs.rm(self.storage_path, recursive=True)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Could not find output file {self.output_filepath}"
            )
        return result

    def with_(self, **kwargs):
        """
        Convenience method for creating a copy of the executor with
        new attributes.
        """
        instance = copy(self)
        for k, v in kwargs.items():
            setattr(instance, k, v)
        return instance
