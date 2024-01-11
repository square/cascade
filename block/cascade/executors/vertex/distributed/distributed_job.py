from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import json
import logging
import os
import pickle
import socket
from subprocess import Popen, check_call
import sys
import time
from typing import Callable, List
import warnings

import gcsfs

VERTEX_DASHBOARD_PORT = "8888"


@dataclass
class DistributedJobBase(ABC):
    """
    Abstract base class for distributed jobs.
    To define a new distributed job, inherit from this class
    and implement the `run` method.
    """

    @staticmethod
    def get_pool_number():
        """
        Parse a Vertex job instance's CLUSTER_SPEC environment variable
        to determine whether this job is being run on a `chief`, `worker`,
        `parameter_server` or `evaluator`.

        See docs: https://cloud.google.com/vertex-ai/docs/training/distributed-training#cluster-spec-format

        Some cluster workloads - e.g. Dask - do not use evaluators and parameter servers.

        """  # noqa: E501

        if "CLUSTER_SPEC" not in os.environ:
            warnings.warn(
                "Did not find CLUSTER_SPEC in environment. CLUSTER_SPEC is expected to "
                "be in environment if running on a Vertex AIP cluster. "
                "https://cloud.google.com/vertex-ai/docs/training/distributed-training#cluster-spec-format"
            )
            return None

        try:
            clusterspec = json.loads(os.environ.get("CLUSTER_SPEC"))
        except json.JSONDecodeError as e:
            logging.error(
                "Found CLUSTER_SPEC in environment but cannot parse it as JSON."
            )
            raise e

        workerpool = clusterspec.get("task", {}).get("type", "")
        # e.g. "workerpool0", "workerpool1"
        workerpool_number = workerpool.replace("workerpool", "")  # e.g. 0, 1
        return int(workerpool_number)

    def run_function(self, dump_output=True):
        """
        Runs the function provided at initialization and optionally
        dumps the output to GCS.
        """
        logging.info("Starting user code execution")
        result = self.func()
        output_path = f"{self.storage_path}/output.pkl"

        if dump_output:
            logging.info(f"Saving output of task to {output_path}")
            fs = gcsfs.GCSFileSystem()
            with fs.open(output_path, "wb") as f:
                pickle.dump(result, f)

    def run(self, func: Callable, storage_path: str):
        """
        Initializes the function and storage path for the distributed job.
        It is necessary to initialize via the run method as the function
        and storage path are unknown at initialize time (when the resource
        object is created).
        """
        self.func = func
        self.storage_path = storage_path
        self._run()

    @abstractmethod
    def _run(self):
        """To be implemented by child classes, this method is intended as the
        entrypoint for running distributed jobs.

        An example _run method for a distributed job that would run a startup
        script on both chief and worker nodes and then execute the job function
        on the chief node.

        task = self.get_pool_number()
        if task == 0:  # on chief
            self.start_chief()
            self.run_function()

        elif task == 1:  # on worker
            self.start_worker()
        """
        pass


@dataclass
class DistributedJob(DistributedJobBase):
    """
    A basic distributed job that launches a cluster without
    any preconfiguration or startup code apart from what Vertex AI provides, see
    https://cloud.google.com/vertex-ai/docs/training/distributed-training
    """

    def _run(self):
        """
        Executes the function and only dumps output from the chief node.
        """
        task = self.get_pool_number()
        if task == 0:  # on chief
            self.run_function(dump_output=True)

        elif task == 1:  # on worker
            self.run_function(dump_output=False)


@dataclass
class DaskJob(DistributedJobBase):
    """
    Configure a Dask Job.

    Parameters
    ----------
    chief_port: str
        A Job object. Port on which scheduler should listen. Be sure to supply a
        string and not an integer. Defaults to "8786".

    scheduler_cli_args: list
       Additional arguments to pass to scheduler process in format
       ['--arg1', 'arg1value', '--arg2', 'arg2value']. Defaults to [].

    worker_cli_args: list
        Additional arguments to pass to worker processes in format
        ['--arg1', 'arg1value', '--arg2', 'arg2value']. Defaults to [].
    """

    chief_port: str = "8786"
    scheduler_cli_args: List[str] = field(default_factory=list)
    worker_cli_args: List[str] = field(default_factory=list)
    logger: logging.Logger = None

    @property
    def chief_ip(self) -> str:
        """
        Gets the IP of the chief host.

        If run on chief, determines the IP of the host on the local network.

        If run on worker, waits for that file to materialize on GCS and returns
        the IP of the chief host.
        """
        task = self.get_pool_number()

        fs = gcsfs.GCSFileSystem()
        if task == 0:  # on chief
            host_name = socket.gethostname()
            return socket.gethostbyname(host_name)
        else:

            chief_ip_file = self.get_chief_ip_file()

            # workers (and potentially evaluators, parameter servers not yet used)
            # look for the file on startup
            times_tried = 0
            retry_delay = 5  # seconds
            max_retries = 36  # 36 tries * 5 seconds = 3 mins
            while times_tried <= max_retries:
                try:
                    with fs.open(chief_ip_file, "r") as f:
                        return f.read().rstrip("\n")
                except FileNotFoundError:
                    if times_tried == max_retries:
                        raise TimeoutError(
                            "Timed out waiting for chief to stage IP file."
                        )
                    self.logger.info(
                        f"waiting for scheduler IP file to be ready at {chief_ip_file}"
                    )
                    times_tried += 1
                    time.sleep(retry_delay)

    def __setstate__(self, state):
        self.__dict__.update(state)

        pool_number = self.get_pool_number()
        if pool_number is not None:

            machine_type = "chief" if not pool_number else f"worker{pool_number}"

            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False

            handler = logging.StreamHandler(stream=sys.stdout)
            handler.setLevel(logging.INFO)

            formatter = logging.Formatter(
                f"%(asctime)s {machine_type} %(levelname)s: %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def get_chief_ip_file(self) -> str:
        """
        Path on GCS at which file containing chief IP is
        expected to be found.
        """
        return os.path.join(self.storage_path, "chief_ip.txt")

    def get_chief_address(self) -> str:
        """
        Return IP and port of chief in format 000.000.000.000:0000
        """

        return f"{self.chief_ip}:{self.chief_port}"

    def start_chief(self):
        """
        Start the Dask scheduler.

        Pass `scheduler_cli_args` at object creation time in format
        `DaskJob(scheduler_cli_args = ['--arg1', 'arg1value', '--arg2', 'arg2value'])`
        to modify worker process startup.
        """

        chief_ip_file = self.get_chief_ip_file()

        fs = gcsfs.GCSFileSystem()
        with fs.open(chief_ip_file, "w") as f:
            f.write(self.chief_ip)

        self.logger.info(f"The scheduler IP is {self.chief_ip}")

        # This allows users (who invoke Client in their code) point their
        # Dask Client at a file called `__scheduler__`
        # i.e. distributed.Client(scheduler_file="__scheduler__")
        with open("__scheduler__", "w") as file:
            json.dump({"address": self.get_chief_address()}, file)

        Popen(
            [
                "dask",
                "scheduler",
                "--protocol",
                "tcp",
                "--port",
                self.chief_port,
                *self.scheduler_cli_args,
                "--dashboard-address",
                f":{VERTEX_DASHBOARD_PORT}",
            ],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def start_worker(self):
        """
        Starts a Dask worker process.

        Pass `worker_cli_args` at object creation time in format
        `DaskJob(worker_cli_args = ['--arg1', 'arg1value', '--arg2', 'arg2value'])`
        to modify worker process startup.
        """
        self.logger.info(f"Chief Ip: {self.chief_ip}")
        check_call(
            [
                "dask",
                "worker",
                self.get_chief_address(),
                *self.worker_cli_args,
            ],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def _run(self):
        """
        Runs startup on chief and worker nodes to set up Dask cluster.
        Runs the function on the chief (scheduler) node and dumps output to GCS.
        """
        self.logger.info("Starting dask run function")
        task = self.get_pool_number()
        if task == 0:  # on chief
            self.start_chief()
            self.run_function(dump_output=True)

        elif task == 1:  # on worker
            self.start_worker()
