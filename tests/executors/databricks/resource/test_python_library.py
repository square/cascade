from unittest.mock import patch
from block_cascade.executors.databricks.resource import DatabricksPythonLibrary


def test_python_library_model_dump():
    library = DatabricksPythonLibrary(name="example-package", version="1.2.3", repo="https://example.com/pypi")
    expected_output = {
        "pypi": {
            "package": "example-package==1.2.3",
            "repo": "https://example.com/pypi"
        }
    }
    assert library.model_dump() == expected_output

    library = DatabricksPythonLibrary(name="example-package", infer_version=False)
    expected_output = {
        "pypi": {
            "package": "example-package"
        }
    }
    assert library.model_dump() == expected_output

def test_python_library_infer_version():
    with patch("block_cascade.executors.databricks.resource.importlib.metadata.version") as mock_version:
        mock_version.return_value = "4.5.6"
        library = DatabricksPythonLibrary(name="example-package", infer_version=True)
        assert library.version == "4.5.6"