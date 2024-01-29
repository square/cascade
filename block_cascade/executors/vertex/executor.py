from dataclasses import dataclass
import logging
import os
from pathlib import Path
import pickle
import time
from typing import Callable, Mapping, Optional, Union

import cloudpickle
import gcsfs
from google.cloud import aiplatform_v1beta1 as aiplatform
from google.cloud.aiplatform_v1beta1.types import job_state

from block_cascade.concurrency import run_async
from block_cascade.executors.executor import Executor
from block_cascade.executors.vertex.distributed.distributed_job import DaskJob
from block_cascade.executors.vertex.job import VertexJob
from block_cascade.executors.vertex.resource import GcpResource
from block_cascade.executors.vertex.tune import Tune, TuneResult
from block_cascade.gcp.monitoring import log_quotas_for_resource
from block_cascade.utils import PREFECT_VERSION, maybe_convert

if PREFECT_VERSION == 2:
    from block_cascade.prefect.v2 import get_current_deployment, get_storage_block
else:
    get_storage_block = None
    get_current_deployment = None


class VertexError(Exception):
    pass


class VertexCancelledError(Exception):
    pass


@dataclass
class Status:
    state: job_state.JobState
    message: str

    @property
    def is_executing(self):
        return self.state in {
            job_state.JobState.JOB_STATE_UNSPECIFIED,
            job_state.JobState.JOB_STATE_QUEUED,
            job_state.JobState.JOB_STATE_PENDING,
            job_state.JobState.JOB_STATE_RUNNING,
            job_state.JobState.JOB_STATE_PAUSED,
        }

    @property
    def is_cancelled(self):
        return self.state in {
            job_state.JobState.JOB_STATE_CANCELLING,
            job_state.JobState.JOB_STATE_CANCELLED,
        }

    @property
    def is_succesful(self):
        return self.state is job_state.JobState.JOB_STATE_SUCCEEDED


class VertexExecutor(Executor):
    def __init__(
        self,
        resource: GcpResource,
        func: Callable,
        name: str = None,
        job: VertexJob = None,
        tune: Tune = None,
        dashboard: bool = False,
        web_console: bool = False,
        labels: Optional[Mapping[str, str]] = None,
        logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None,
        code_package: Optional[Path] = None,
    ):
        """Executor to submit VertexJobs as CustomJobs in Vertex AI
        The lifecycle of an executor is:
        * initialize the executor
        * call the run method
            * this in turn creates a VertexJob, stages the function and data,
                attempts to start the job by calling VertexAI's CustomJob API
                and monitors the status of the job
            * the _start method is reposible for managing the lifecycle of the VertexJob

        Parameters
        ----------
        resource: GcpResource
            The environment in which to create and run a VertexJob
        func: Callable
            The function to execute
            Note: this needs to be a Callable that expects no arguments
            and has a __name__ attribute; use cascade.utils.wrapped_partial
            to prepare a function for execution
        job: VertexJob
            It is possible to pass an existing VertexJob to the executor; this is not
            recomended. The run method of the executor will create a new VertexJob if
            no job is passed.
        dashboard: bool
            Whether to enable access to the Dask dashboard in VertexAI, defaults
            to False
        web_console: bool
            Whether to enable access to the web console in VertexAI, defaults to False
        labels: Dict[str, str], Optional
            User defined metadata to attatch to VertexJobs.
        logger: Union[logging.Logger, logging.LoggerAdapter], Optional
            A configured logger for logging messages.
        code_package: Optional[Path]
            An optional path to the first party code that your remote execution needs.
            This is only necessary if the following conditions hold true:
                - The function is desired to run in Vertex AI
                - The function is not being executed from a Prefect2 Cloud Deployment
                - The function references a module that is not from a third party
                dependency, but from the same package the function is a member of.
        """
        super().__init__(func=func)
        self.resource = resource
        self.dashboard = dashboard
        self.web_console = web_console
        self.job = job
        self.tune = tune
        self.labels = labels
        self.name = name

        if code_package and code_package.is_file():
            raise RuntimeError(
                f"{code_package} references a file. "
                "Please reference a directory representing a Python package."
            )
        self.code_package = code_package

        # the name is not known at executor initialization,
        # it is set when the job is submitted
        self._name = name
        self._fs = gcsfs.GCSFileSystem()
        self._storage_location = self.resource.environment.storage_location
        self._logger = logger
        if not self._logger:
            self._logger = logging.getLogger(__name__)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def vertex(self):
        """
        Returns a Vertex client; refreshes the client for each usage
        This seems excessive but we've had issues with authentication expiring
        """
        region = self.job.resource.environment.region
        client_options = {"api_endpoint": f"{region}-aiplatform.googleapis.com"}
        return aiplatform.JobServiceClient(client_options=client_options)

    @property
    def display_name(self):
        if self.name is not None:
            return self.name
        if self.tune is None:
            return f"{self.func.__name__}-cascade"
        return f"{self.func.__name__}-hyperparameter-tuning"

    @property
    def distributed_job_path(self):
        return os.path.join(self.storage_path, "distributed_job.pkl")

    @property
    def code_path(self):
        return os.path.join(self.storage_path, "code")

    def create_job(self) -> VertexJob:
        """
        create a VertexJob to be run by this executor
        when we move to 3.10 can use matching
        """

        package_path = None
        if not self.code_package and PREFECT_VERSION == 2:
            self._logger.info(
                "Checking if flow is running from a Prefect 2 Cloud deployment."
            )
            deployment = get_current_deployment()
            if deployment:
                storage = get_storage_block()
                if "/" in deployment.entrypoint:
                    module_name = deployment.entrypoint.split("/")[0]
                elif ":" in deployment.entrypoint:
                    module_name = deployment.entrypoint.split(":")[0]
                else:
                    module_name = deployment.entrypoint

                module_name = module_name.lstrip("/")
                bucket = storage.data["bucket_path"]
                deployment_path = deployment.path.rstrip("/")

                package_path = f"{bucket}/{deployment_path}/{module_name}"
                self._logger.info(
                    f"Code package from deployment is located at gs://{package_path}"
                )
        elif self.code_package:
            upload_path = os.path.join(self.code_path, self.code_package.name)
            fs = gcsfs.GCSFileSystem()
            self._logger.info(f"Uploading first party package to {upload_path}")
            fs.upload(str(self.code_package), upload_path, recursive=True)
            package_path = upload_path

        dashboard = self.dashboard or isinstance(self.resource.distributed_job, DaskJob)
        return VertexJob(
            display_name=self.display_name,
            resource=self.resource,
            storage_path=self.storage_path,
            tune=self.tune,
            dashboard=dashboard,
            web_console=self.web_console,
            labels=self.labels,
            code_package=package_path,
        )

    def _stage_distributed_job(self):
        """
        Stages a distributed job object in GCS; this is done instead of staging the
        function as the distributed job requires the function to be included in the
        distributed job object to perform additional setup on the cluster before
        function execution
        """
        with self.fs.open(self.distributed_job_path, "wb") as f:
            cloudpickle.dump(self.resource.distributed_job, f)

    def _get_status(self):
        name = self.name
        if "hyperparameter" in name:
            response = self.vertex.get_hyperparameter_tuning_job(name=name)
        else:
            response = self.vertex.get_custom_job(name=name)
        return Status(response.state, response.error)

    def _run(self):
        """
        Runs a task and return the result, called from
        the public run function.
        Submits the job to Vertex and monitors its status by polling the API.
        Returns the result of the job.
        """

        if self.resource.distributed_job is not None:
            self._stage_distributed_job()

        self._stage()

        custom_job_name = self._start()
        self.name = custom_job_name

        status = self._get_status()
        while status.is_executing:
            time.sleep(30)
            status = self._get_status()

        if status.is_cancelled:
            raise VertexCancelledError(
                f"Job {self.name} was cancelled: {status.message}"
            )

        if not status.is_succesful:
            raise VertexError(f"Job {self.name} failed: {status.message}")

        return self._result()

    def _start(self) -> name:
        """
        Create and start a job in Vertex.
        Returns the name of the job as returned by the Vertex CustomJobs API
        """

        # if the job does not exist, create it
        if self.job is None:
            self.job = self.create_job()

        try:
            run_async(log_quotas_for_resource(self.job.resource))
        except Exception as e:
            self._logger.warning(e)
            pass

        # create the payload for the custom job
        custom_job_payload = self.job.create_payload()

        # pull the gcp_environment from the job object
        gcp_environment = self.job.resource.environment
        project = gcp_environment.project
        region = gcp_environment.region

        # the resource name of the Location to create the custom job in
        parent = f"projects/{project}/locations/{region}"

        # submit the job to CutomJob endpoint
        if self.job.tune is None:
            resp = self.vertex.create_custom_job(
                parent=parent, custom_job=custom_job_payload
            )
            self._logger.info(f"Created a remote vertex job: {resp}")
        else:
            resp = self.vertex.create_hyperparameter_tuning_job(
                parent=parent, hyperparameter_tuning_job=custom_job_payload
            )
            self._logger.info(
                f"Created a remote vertex hyperparameter tuning job: {resp}"
            )

        _, _, _, location, _, job_id = resp.name.split("/")
        path = f"locations/{location}/training/{job_id}"

        url_log = f"Logs for remote job can be found at https://console.cloud.google.com/vertex-ai/{path}"
        self._logger.info(url_log)

        # return the name of the job in Vertex for use monitoring status
        # and fetching results
        return resp.name

    def _result(self):
        output_filepath = self.output_filepath
        # We have to build hyperparameter tuning result from the Vertex api, not
        # the task and we save it to the task output here for consistency
        if "hyperparameter" in self.name:
            result = self._tune_result()
            with self.fs.open(output_filepath, "wb") as f:
                pickle.dump(result, f)
            return result

        # For anything else we can just load the result from the output
        try:
            with self.fs.open(output_filepath, "rb") as f:
                return pickle.load(f)
        except ValueError:
            self._logger.warning(
                f"Failed to load the output from succesful job at {output_filepath}"
            )
            raise

    def _tune_result(self):
        response = self.vertex.get_hyperparameter_tuning_job(name=self.name)

        reverse = "MAXIMIZE" in str(response.study_spec.metrics[0].goal)
        trials = response.trials
        trials = sorted(
            trials,
            key=lambda trial: trial.final_measurement.metrics[0].value,
            reverse=reverse,
        )
        flattened = [
            {
                "trial_id": maybe_convert(t.id),
                "metric": maybe_convert(t.final_measurement.metrics[0].value),
                **{
                    param.parameter_id: maybe_convert(param.value)
                    for param in t.parameters
                },
            }
            for t in trials
            if t.state.name == "SUCCEEDED"
        ]
        best_trial = flattened[0]
        metric = best_trial["metric"]
        hyperparameters = {
            key: val
            for key, val in best_trial.items()
            if key not in ("trial_id", "metric")
        }
        return TuneResult(metric, hyperparameters, flattened)
