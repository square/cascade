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


def test_databricks_resource_string_library_conversion():
    from block_cascade.executors.databricks.resource import DatabricksResource
    
    resource = DatabricksResource(
        storage_location="s3://test-bucket/cascade",
        python_libraries=["test-package"]
    )
    
    assert len(resource.python_libraries) == 1
    assert isinstance(resource.python_libraries[0], DatabricksPythonLibrary)
    assert resource.python_libraries[0].name == "test-package"
    
    resource = DatabricksResource(
        storage_location="s3://test-bucket/cascade",
        python_libraries=[
            "package1", 
            DatabricksPythonLibrary(name="package2", version="1.0.0")
        ]
    )
    
    assert len(resource.python_libraries) == 2
    assert isinstance(resource.python_libraries[0], DatabricksPythonLibrary)
    assert isinstance(resource.python_libraries[1], DatabricksPythonLibrary)
    assert resource.python_libraries[0].name == "package1"
    assert resource.python_libraries[1].name == "package2"
    assert resource.python_libraries[1].version == "1.0.0"


def test_databricks_resource_string_with_version_conversion():
    from block_cascade.executors.databricks.resource import DatabricksResource
    
    resource = DatabricksResource(
        storage_location="s3://test-bucket/cascade",
        python_libraries=["cloudpickle==0.10.0"]
    )
    
    assert len(resource.python_libraries) == 1
    assert isinstance(resource.python_libraries[0], DatabricksPythonLibrary)
    assert resource.python_libraries[0].name == "cloudpickle"
    assert resource.python_libraries[0].version == "0.10.0"
    
    resource = DatabricksResource(
        storage_location="s3://test-bucket/cascade",
        python_libraries=[
            "numpy==1.22.4",
            "pandas==2.0.0",
            DatabricksPythonLibrary(name="scikit-learn", version="1.2.2")
        ]
    )
    
    assert len(resource.python_libraries) == 3
    assert isinstance(resource.python_libraries[0], DatabricksPythonLibrary)
    assert isinstance(resource.python_libraries[1], DatabricksPythonLibrary)
    assert isinstance(resource.python_libraries[2], DatabricksPythonLibrary)
    assert resource.python_libraries[0].name == "numpy"
    assert resource.python_libraries[0].version == "1.22.4"
    assert resource.python_libraries[1].name == "pandas"
    assert resource.python_libraries[1].version == "2.0.0"
    assert resource.python_libraries[2].name == "scikit-learn"
    assert resource.python_libraries[2].version == "1.2.2"