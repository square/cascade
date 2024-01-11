from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Union


class Scale(Enum):
    UNIT_LINEAR_SCALE = auto()
    UNIT_LOG_SCALE = auto()
    UNIT_REVERSE_LOG_SCALE = auto()


@dataclass
class ParamDiscrete:
    name: str
    values: List[float]


@dataclass
class ParamCategorical:
    name: str
    values: List[str]


@dataclass
class ParamDouble:
    name: str
    min: float
    max: float
    scale: Optional[Scale] = None


@dataclass
class ParamInteger:
    name: str
    min: float
    max: float
    scale: Optional[Scale] = None


@dataclass
class Tune:
    metric: str
    params: List[Union[ParamDiscrete, ParamCategorical, ParamInteger, ParamDouble]]
    goal: str = "MAXIMIZE"
    trials: int = 1
    parallel: int = 1
    resume_previous_job_id: Optional[str] = None
    algorithm: Optional[str] = None


@dataclass
class TuneResult:
    # TODO because AIP tune only reports the "main" metric, the trials df
    # only contains that metric as well. Whenever we start to want to inspect
    # these ping @baxen to come back to have this actually go find them all
    metric: float
    hyperparameters: dict
    trials: List[dict]
