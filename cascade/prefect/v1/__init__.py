import logging
from typing import Union

import prefect
from prefect.backend.flow import FlowView


def get_from_prefect_context(attr: str, default: str = "") -> str:
    return prefect.context.get(attr, default)


def is_prefect_cloud_deployment() -> bool:
    flow_id = get_from_prefect_context("flow_id")
    if not flow_id:
        return False

    try:
        FlowView.from_flow_id(flow_id)
        return True
    except prefect.exceptions.ClientError:
        return False


def get_prefect_logger(name: str = "") -> Union[logging.LoggerAdapter, logging.Logger]:
    return prefect.context.get("logger")
