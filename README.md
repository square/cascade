# cascade

Cascade is a library for submitting and managing jobs across multiple cloud environments. It is designed to integrate seamlessly into existing Prefect workflows or can be used as a standalone library.

## Getting Started

### Installation
  
  ```bash 
  poetry add block-cascade
  ```
or 
```
pip install block-cascade
```

### Example Usage in a Prefect flow

```python
from block_cascade import remote
from block_cascade import GcpEnvironmentConfig, GcpMachineConfig, GcpResource

machine_config = GcpMachineConfig("n2-standard-4", 1)
environment_config = GcpEnvironmentConfig(
    project="ds-cash-production",
    region="us-west1",
    service_account=f"ds-cash-production@ds-cash-production.iam.gserviceaccount.com",
    image="us.gcr.io/ds-cash-production/cascade/cascade-test",
    network="projects/603986066384/global/networks/neteng-shared-vpc-prod"
)
gcp_resource = GcpResource(
    chief=machine_config,
    environment=environment_config,
)

@remote(resource=gcp_resource)
def addition(a: int, b: int) -> int:
    return a + b

result = addition(1, 2)
assert result == 3
```

### Configuration
Cascade supports defining different resource requirements via a configuration file titled either cascade.yaml or cascade.yml. This configuration file must be located in the working directory of the code execution to be discovered at runtime.

```yaml
calculate:
  type: GcpResource
  chief:
    type: n1-standard-1
You can even define a default configuration that can be overridden by specific tasks to eliminate redundant definitions.

default:
    GcpResource:
        environment:
            project: ds-cash-dev
            service_account: ds-cash-production@ds-cash-production.iam.gserviceaccount.com
            region: us-central-1
        chief:
            type: n1-standard-4
calculate:
    type: GcpResource
    environment:
        project: ds-cash-production
    chief:
        count: 2
```

### Authorization
Cascade requires authorization both to submit jobs to either GCP or Databricks and to stage picklied code to a cloud storage bucket. In the GCP example below, an authorization token is obtained via IAM by running the following command:

```bash
gcloud auth login --update-adc
```
No additional configuration is required in your application's code to use this token.

However, for authenticating to Databricks and AWS you will need to provide a token and secret key respectively. These can be passed directly to the `DatabricksResource` object or set as environment variables. The following example shows how to provide these values in the configuration file.