import logging
import pickle
import sys

import cloudpickle
import gcsfs
from hypertune import HyperTune

from block_cascade.utils import parse_hyperparameters

INPUT_FILENAME = "function.pkl"
DISTRIBUTED_JOB_FILENAME = "distributed_job.pkl"
OUTPUT_FILENAME = "output.pkl"

# Clear the Prefect Handler until that
# dependency gets removed.
logging.getLogger().handlers.clear()
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)

logger = logging.getLogger(__name__)


def run():
    """
    Entrypoint to run a staged pickled function in VertexAI.
    This module is invoked as a Python script and the values for
    "path" and "hyperparameters" are passed as arguments.

    See cascade.executors.vertex.job.VertexJob._create_container_spec
    """

    path_prefix, distributed_job, code_path, hyperparameters = (
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
        sys.argv[4:],
    )

    staged_path = f"{path_prefix}/{INPUT_FILENAME}"
    distributed_job_path = f"{path_prefix}/{DISTRIBUTED_JOB_FILENAME}"
    output_path = f"{path_prefix}/{OUTPUT_FILENAME}"

    fs = gcsfs.GCSFileSystem()
    if code_path:
        logger.info(f"Fetching {code_path} and added to sys.path.")
        fs.get(code_path, ".", recursive=True)
        sys.path.insert(0, ".")

    with fs.open(staged_path, "rb") as f:
        func = cloudpickle.load(f)

    hyperparameters = parse_hyperparameters(hyperparameters)
    # If we received hyperparameters as args, we infer we are doing a tune
    # the result of the function is the metric and we report that to Vertex
    if hyperparameters:
        logger.info(f"Starting execution with hyperparameters: {hyperparameters}")
        metrics = func(hyperparameters=hyperparameters)
        logger.info(f"Reporting metrics to hyperparameter tune: {metrics}")
        htune = HyperTune()
        for k, v in metrics.items():
            htune.report_hyperparameter_tuning_metric(k, v)

    # If the job is a distributed job (including Dask and Torch)
    elif distributed_job == "True":
        logger.info("Starting execution of distributed job")
        with fs.open(distributed_job_path, "rb") as f:
            distributed_job = cloudpickle.load(f)
        distributed_job.run(func=func, storage_path=path_prefix)

    # If neither of the above are true,  we assume this is a regular
    # single node job and the result of the function is saved
    # to storage to be read by the submitting call
    else:
        logger.info("Starting execution")
        result = func()
        logger.info(f"Saving output of task to {output_path}")
        with fs.open(output_path, "wb") as f:
            pickle.dump(result, f)


if __name__ == "__main__":
    # This module is run as main in order to execute a task on a worker
    run()
