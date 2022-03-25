# DLT PII Firewall

> :warning: **THIS PROJECT IS PROVIDED AS-IS WITHOUT ANY GUARANTEES**: we make no claims as to the accuracy of the PII detection provided here, and if you use it it is **YOUR RESPONSIBILITY** to ensure that the example regexes and detection/redaction/tagging meets your internal, legal or regulatory requirements.

### Using [Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) to identify and redact potential PII data!

[Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake.

### Setup

To get this pipeline running on your environment, please use the following steps:

1. Clone the Github Repo using our Repos for Git Integration (see the docs for [AWS](https://docs.databricks.com/repos/index.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/repos/), [GCP](https://docs.gcp.databricks.com/repos/index.html)). 





### Run the Job

When everything is set up correctly you should see something like this...

#### 1. Multi-step job to automatically detect, redact and tag PII:

#### 2. DLT pipeline to automatically detect and redact PII:

![image](https://user-images.githubusercontent.com/43955924/160136979-a16fc3c8-1fbe-4e0f-8660-24b4e8f52c0e.png)

![image](https://user-images.githubusercontent.com/43955924/160137248-386e649e-d1a8-4c24-adeb-46bf734d7fad.png)

#### 3. Example of the column-level PII tagging applied:

#### 4. Example of the redacted output table:

In the [notebooks folder](notebooks/) you will find the following:

* [00_generate_data.py](notebooks/00_generate_data.py) - a notebook that can be used to generate fake PII data
* [01_observability.py](notebooks/01_observability.py) - a Delta Live Table notebook that creates an observability pipeline for our DLT workloads
* [02_detect_and_redact_pii.py](notebooks/02_detect_and_redact_pii.py) - a Delta Live Table notebook that uses the expectations and actions defined in the same JSON file [here](expectations/pii_detection.json) to detect and redact PII
* [03_tag_pii.py](notebooks/03_tag_pii.py) - a notebook that tags the generated databases/tables/columns based on the PII detected and actions taken
* [04_cleanup.py](notebooks/04_cleanup.py) - a notebook that can be used to clean up the input, storage and output data so that it can be re-tested
