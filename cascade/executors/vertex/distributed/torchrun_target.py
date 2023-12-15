import json
import logging
import os
import pickle
import sys

import cloudpickle
import gcsfs
import torch
import torch.distributed as dist
from torch.distributed.elastic.multiprocessing.errors import record


class InvalidReturnDictionaryError(Exception):
    """User returned dictionaries must contain the key MODEL_STATE. Raise this exception
    if the user returned dict is not compliant
    """

    def __init__(self):
        message = (
            "User-defined training function must return a dictionary with the "
            "key `MODEL_STATE` that contains a Pytorch state_dict"
        )
        super().__init__(message)


class DistributedSetup:
    """Context Manager to setup Pytorch distributed environment to enable
    distributed training

    If the job fails, tear down the process group.
    """

    def __init__(self) -> None:
        cluster_spec = json.loads(os.environ["CLUSTER_SPEC"])
        master_hostname, master_port = cluster_spec["cluster"]["workerpool0"][0].split(
            ":"
        )

        os.environ["MASTER_ADDR"] = str(master_hostname)
        os.environ["MASTER_PORT"] = str(master_port)

        torch.cuda.set_device(int(os.environ["LOCAL_RANK"]))

    def __enter__(self) -> None:
        dist.init_process_group("nccl")

    def __exit__(self, *args, **kwargs) -> None:
        dist.destroy_process_group()


@record
def run_job_function() -> None:
    """Runs the wrapped, pickled, user-defined function. Wraps the user-defined function
    in the necessary boilerplate to enable distributed training
    """
    fs = gcsfs.GCSFileSystem()

    input_filepath = sys.argv[1]
    output_filepath = sys.argv[2]

    with fs.open(input_filepath, "rb") as f:
        job = cloudpickle.load(f)

    with DistributedSetup():
        logging.info("Starting user code execution")
        snapshot = job.func()

    # This is a "Look before you leap" check to make sure the user-returned value is
    # something we expect: a dictionary that at least contains the key "MODEL_STATE"
    if not isinstance(snapshot, dict) or "MODEL_STATE" not in snapshot:
        raise InvalidReturnDictionaryError()

    # Save the model state to job.output. All processes will have a consistent model
    # state so we only need to save the model once, from the Rank 0 process
    if os.environ["RANK"] == "0":
        model_state_dict = snapshot["MODEL_STATE"]

        # "Moving" all of the model state tensors to the CPU allows the state dict
        # to be accessed as expected downstream. This does introduce an abstraction leak
        # since it requires the user to pass a specifically formatted return value
        model_state_cpu = {k: v.cpu() for k, v in model_state_dict.items()}
        snapshot["MODEL_STATE"] = model_state_cpu

        with fs.open(output_filepath, "wb") as f:
            pickle.dump(snapshot, f)
            print(f"Result object saved at {output_filepath}")


if __name__ == "__main__":
    run_job_function()
