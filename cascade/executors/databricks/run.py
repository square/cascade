import logging
import os
import pickle
import sys

try:
    import cloudpickle
except ImportError:
    import pickle as cloudpickle  # Databricks Runtime 11+ renames cloudpickle to pickle...  # noqa: E501

INPUT_FILENAME = "function.pkl"
OUTPUT_FILENAME = "output.pkl"


logger = logging.getLogger(__name__)


def run():
    bucket_location, storage_key = sys.argv[1], sys.argv[2]
    mountpoint = f"/mnt/cascade/{storage_key}/"
    dbfs_mountpoint = "/dbfs" + mountpoint

    dbutils.fs.mount(  # dbutils is populated in cluster namespace without import  # noqa: E501, F821
        bucket_location.replace("s3://", "s3n://"),
        mountpoint,
    )
    try:
        with open(os.path.join(dbfs_mountpoint, INPUT_FILENAME), "rb") as f:
            func = cloudpickle.load(f)

        logger.info("Starting execution")
        result = func()

        logger.info(f"Saving output of task to {bucket_location}/{OUTPUT_FILENAME}")
        try:
            with open(os.path.join(dbfs_mountpoint, OUTPUT_FILENAME), "wb") as f:
                pickle.dump(result, f)
        except RuntimeError as e:
            logger.error(
                "Failed to serialize user function return value. Be sure not to return "
                "Spark objects from user functions. For example, you should convert "
                "Spark dataframes to Pandas dataframes before returning."
            )
            raise e
    finally:
        dbutils.fs.unmount(mountpoint)  # noqa: F821


if __name__ == "__main__":
    # This module is run as main in order to execute a task on a worker
    run()
