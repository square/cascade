import logging
import pickle
import sys
import os

try:
    import cloudpickle
except ImportError:
    import pickle as cloudpickle  # Databricks Runtime 11+ renames cloudpickle to pickle...  # noqa: E501

INPUT_FILENAME = "function.pkl"
OUTPUT_FILENAME = "output.pkl"


logger = logging.getLogger(__name__)


def run():
    storage_location, _ = sys.argv[1], sys.argv[2]
    
    # Detect storage type: S3 (cluster compute) or Unity Catalog Volumes (serverless compute)
    if storage_location.startswith("s3://"):
        _run_s3(storage_location)
    elif storage_location.startswith("/Volumes/"):
        _run_volumes(storage_location)
    else:
        raise ValueError(
            f"Unsupported storage location: {storage_location}. "
            "Must be /Volumes/ (serverless) or s3:// (cluster compute)."
        )


def _run_s3(bucket_location: str):
    """Run with S3 storage backend."""
    import boto3
    
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


def _run_volumes(storage_location: str):
    """Run with Unity Catalog Volumes storage backend.
    
    Unity Catalog Volumes are accessed as regular filesystem paths from within
    Databricks clusters. They provide proper security and permissions through
    Unity Catalog governance.
    """
    input_path = os.path.join(storage_location, INPUT_FILENAME)
    output_path = os.path.join(storage_location, OUTPUT_FILENAME)
    
    try:
        # Read pickled function directly from filesystem
        with open(input_path, "rb") as f:
            func = cloudpickle.load(f)
        
        logger.info("Starting execution")
        result = func()
        
        logger.info(f"Saving output of task to {output_path}")
        try:
            with open(output_path, "wb") as f:
                pickle.dump(result, f)
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