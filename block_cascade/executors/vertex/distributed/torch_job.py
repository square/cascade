from dataclasses import dataclass
import json
import logging
import os
from subprocess import check_call
import sys
from typing import List

from block_cascade.executors.vertex.distributed.distributed_job import (
    DistributedJobBase,
)
from block_cascade.utils import INPUT_FILENAME, OUTPUT_FILENAME

MASTER_PORT = "3333"  # Hardcode MASTER_PORT for Vertex AI proxy compatibility
RDZV_ID = "123456"  # Can be any random string
RDZV_BACKEND = "c10d"  # c10d is the Pytorch-preferred backend, more info in the Pytorch "Rendezvous" docs  # noqa: E501


@dataclass
class TorchJob(DistributedJobBase):
    """
    Configure and run a distributed Pytorch training Job.
    """

    # input data folder contents from gcs will be downloaded to /app/data
    input_data_gcs_path: str = None

    def get_data_from_gcs(self, gcs_path, local_destination="/app/data") -> None:
        if not os.path.exists(local_destination):
            os.makedirs(local_destination)
        check_call(
            [
                "gsutil",
                "-m",
                "cp",
                "-r",
                gcs_path,
                local_destination,
            ]
        )
        return None

    def get_cli_args(self) -> List[str]:
        """ Get all the args to pass to torchrun. This function takes no input, all
        args are either hardcoded or parsed from environment variables

        torchrun is a console script provided by Pytorch, more information on its use
        and available arguments is available in the Pytorch docs

            torchrun \
            --nproc_per_node=auto \
            --rdzv_id=$RDZV_ID \
            --rdzv_backend=$RDZV_BACKEND \
            --rdzv_endpoint=$RDZV_ADDR:$RDZV_PORT \
            --rdzv_conf=is_host=$(if [[ $RANK != "0" ]]; then echo false;else echo true;fi) \
            --nnodes=$NNODES \
            -m \
            $SCRIPT_PATH \
            $JOB_INPUT
        """  # noqa: E501
        torchrun_target_module_path = (
            "block_cascade.executors.vertex.distributed.torchrun_target"
        )

        cluster_spec = json.loads(
            os.environ["CLUSTER_SPEC"]
        )  # Env var CLUSTER_SPEC injected by vertex

        input_path = f"{self.storage_path}/{INPUT_FILENAME}"
        output_path = f"{self.storage_path}/{OUTPUT_FILENAME}"

        if "workerpool1" in cluster_spec["cluster"]:
            # If there are additional workers beyond the chief node, setup multi-node
            # training
            master_addr, _ = cluster_spec["cluster"]["workerpool0"][0].split(":")

            nnodes = str(len(cluster_spec["cluster"]["workerpool1"]) + 1)

            rank = os.environ["RANK"]  # Env var RANK injected by vertex
            is_chief = str(
                rank == "0"
            ).lower()  # This is the only change in logic between chief and workers

            return [
                "torchrun",
                "--nproc_per_node=auto",
                f"--rdzv_id={RDZV_ID}",
                f"--rdzv_backend={RDZV_BACKEND}",
                f"--rdzv_endpoint={master_addr}:{MASTER_PORT}",
                f"--rdzv_conf=is_host={is_chief}",
                f"--nnodes={nnodes}",
                "-m",
                torchrun_target_module_path,
                input_path,
                output_path,
            ]
        else:
            # Handle the special case where nnodes=="1", i.e. when there is
            # only a master node and no additional workers.
            return [
                "torchrun",
                "--standalone",
                "--nproc_per_node=auto",
                "-m",
                torchrun_target_module_path,
                input_path,
                output_path,
            ]

    def _run(self) -> None:
        """
        Optionally load input data from GCS and then launch the torch run
        utility to run training training.

        There is no switch logic for chief/master and worker nodes:
        this function is run on each training node, then the torchrun utility
        distributes the torchrun target script to each accelerator and runs
        the training script

        torchrun is a console script provided by Pytorch, more information on its use
        here: https://pytorch.org/docs/stable/elastic/run.html
        """
        if self.input_data_gcs_path:
            self.get_data_from_gcs(self.input_data_gcs_path)

        cli_args = self.get_cli_args()
        logging.info(f"Running torchrun with args {cli_args}")
        check_call(
            cli_args,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
