import pytest

from block.cascade.executors.vertex.job import VertexJob
from block.cascade.executors.vertex.tune import ParamDouble, ParamInteger, Tune
from tests.resource_fixtures import gcp_resource, GCP_PROJECT

CONTAINER_SPEC = {
    "image_uri": "us.gcr.io/test_project/cascade",
    "command": [
        "python",
        "-m",
        "cascade.executors.vertex.run",
        "gs://bucket/path/to/file",
        "gs://bucket/path/to/output",
    ],
    "args": [],
}


def test_vertex_job():
    job = VertexJob(
        display_name="test_job",
        resource=gcp_resource,
        storage_path="gs://bucket/path/to/file",
        labels={"hello": "WORLD"},
    )
    payload = job.create_payload()
    assert payload["display_name"] == "test_job"
    assert payload["labels"]["hello"] == "world"

    job_spec = payload["job_spec"]
    service_account = job_spec["service_account"]

    assert service_account == f"{GCP_PROJECT}@{GCP_PROJECT}.iam.gserviceaccount.com"


@pytest.mark.parametrize(
    "key,val",
    [
        ("1key", "val"),
        ("", "val"),
    ],
    ids=[
        "key starts with number",
        "empty key",
    ],
)
def test_invalid_labels_for_vertex_job(key, val):
    with pytest.raises(RuntimeError):
        job = VertexJob(
            display_name="test_job",
            resource=gcp_resource,
            storage_path="gs://bucket/path/to/file",
            labels={key: val},
        )
        job.create_payload()


def test_vertex_tune_job():
    """Test that a tuning job can be successfully created."""

    tune_obj = Tune(
        metric="sum",
        trials=4,
        parallel=2,
        params=[ParamDouble("a", 0, 9.3), ParamInteger("b", 0, 4)],
    )

    tune_job = VertexJob(
        display_name="test_tune_job",
        resource=gcp_resource,
        storage_path="gs://bucket/path/to/file",
        tune=tune_obj,
    )

    payload = tune_job.create_payload()
    assert payload["display_name"] == "test_tune_job"

    assert payload.keys() == {
        "display_name",
        "trial_job_spec",
        "max_trial_count",
        "parallel_trial_count",
        "study_spec",
        "labels",
    }
