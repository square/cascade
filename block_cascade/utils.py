import ast
from functools import partial
import inspect
from inspect import signature
import itertools
import logging
from typing import List
import subprocess
import json

import prefect

logger = logging.getLogger(__name__)


def detect_prefect_version(n) -> int:
    return int(prefect.__version__.split(".")[n])


PREFECT_VERSION, PREFECT_SUBVERSION = detect_prefect_version(0), detect_prefect_version(
    1
)

INPUT_FILENAME = "function.pkl"
DISTRIBUTED_JOB_FILENAME = "distributed_job.pkl"
OUTPUT_FILENAME = "output.pkl"


def get_gcloud_config() -> dict:
    """Get the current gcloud config if available in a user's environment."""
    try:
        # Run the gcloud config list command and parse the output as JSON
        result = subprocess.run(
            ["gcloud", "config", "list", "--format", "json"],
            capture_output=True,
            text=True,
        )
        # Check if the command was executed successfully
        if result.returncode == 0:
            config = json.loads(result.stdout)
        else:
            logger.error("Error listing gcloud configuration:", result.stderr)
            config = dict()
    except Exception as e:
        logger.error("Error listing gcloud configuration:", e)
        config = dict()

    return config


def get_args(obj):
    return list(signature(obj).parameters.keys())


def maybe_convert(arg: str) -> str:
    """Convenience function for parsing hyperparameters"""
    try:
        return ast.literal_eval(arg)
    except ValueError:
        return arg


def parse_hyperparameters(args: List[str]) -> dict:
    # support both --a=3 and --a 3 syntax
    args = list(itertools.chain(*(a.split("=") for a in args)))
    return {
        args[i].lstrip("-"): maybe_convert(args[i + 1]) for i in range(0, len(args), 2)
    }


def _get_object_args(obj: object):
    return list(signature(obj).parameters.keys())


def _infer_base_module(func):
    """
    Inspects the function to find the base module in which it was defined.
    Args:
        func (Callable): a function
    Returns:
        str: the name of the base module in which the function was defined
    """
    func_module = inspect.getmodule(func)
    try:
        base_name, *_ = func_module.__name__.partition(".")
        return base_name
    except AttributeError:
        return None


def wrapped_partial(func, *args, **kwargs):
    """
    Return a partial function, while keeping the original function's name.
    Note: not using functools.update_wrapper here due to incompatability with Prefect 2
    for more details see: https://github.com/squareup/cascade/pull/188
    """
    partial_func = partial(func, *args, **kwargs)
    setattr(partial_func, "__name__", func.__name__)
    return partial_func
