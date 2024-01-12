import logging
import pickle
import sys

import boto3

try:
    import cloudpickle
except ImportError:
    import pickle as cloudpickle  # Databricks Runtime 11+ renames cloudpickle to pickle...  # noqa: E501

INPUT_FILENAME = "function.pkl"
OUTPUT_FILENAME = "output.pkl"


logger = logging.getLogger(__name__)


def run():
    bucket_location, _ = sys.argv[1], sys.argv[2]
    s3_bucket, object_path = bucket_location.replace("s3://", "").split("/", 1)

    try:
        s3 = boto3.resource("s3")
        func = cloudpickle.loads(
            s3.Bucket(s3_bucket)
            .Object(f"{object_path}/{INPUT_FILENAME}")
            .get()["Body"]
            .read()
        )
        logger.info("Starting execution")

        result = func()

        logger.info(f"Saving output of task to {bucket_location}/{OUTPUT_FILENAME}")
        try:
            s3.Bucket(s3_bucket).Object(f"{object_path}/{OUTPUT_FILENAME}").put(
                Body=pickle.dumps(result)
            )
        except RuntimeError as e:
            logger.error(
                "Failed to serialize user function return value. Be sure not to return "
                "Spark objects from user functions. For example, you should convert "
                "Spark dataframes to Pandas dataframes before returning."
            )
            raise e
    except RuntimeError as e:
        logger.error("Failed to execute user function")
        raise e


if __name__ == "__main__":
    # This module is run as main in order to execute a task on a worker
    run()
