import ast
from functools import partial
import importlib
import inspect
from inspect import signature
import itertools
import logging
import pkgutil
import sys
from typing import List

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


def _base_module(func):
    func_module = inspect.getmodule(func)
    base_name, *_ = func_module.__name__.partition(".")
    base = sys.modules[base_name]
    for _, name, _ in pkgutil.walk_packages(base.__path__):
        return importlib.import_module(f"{base_name}.{name}")


def wrapped_partial(func, *args, **kwargs):
    """
    Return a partial function, while keeping the original function's name.
    Note: not using functools.update_wrapper here due to incompatability with Prefect 2
    for more details see: https://github.com/squareup/cascade/pull/188
    """
    partial_func = partial(func, *args, **kwargs)
    setattr(partial_func, "__name__", func.__name__)
    return partial_func
