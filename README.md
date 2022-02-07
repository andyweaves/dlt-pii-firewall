# dlt-pii-detection
A demo of using [Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) to identify and redact potential PII data!

> :warning: **This is a demo and is provided as-is**: we make no claims as to the accuracy of the PII detection provided here, and if you use it it is **YOUR** responsibility to ensure that the example regexes and detection/redaction/tagging pipelines meet your requirements.

**Delta Live Tables (DLT) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake. DLT helps data engineering teams  simplify ETL development and management with declarative pipeline development, automatic data testing, and deep visibility for monitoring and recovery.**

The project is currently designed to be run as a [multi task job](https://docs.databricks.com/data-engineering/jobs/jobs-quickstart.html) on the world's favourite analytics platform... [Databricks](https://databricks.com/)!

In the [notebooks folder](notebooks/) you will find the following:

* [00_generate_data.py](notebooks/00_generate_data.py) - a notebook that can be used to generate fake PII data
* [01_observability.py](notebooks/01_observability.py) - a Delta Live Table notebook that creates an observability pipeline for our DLT workloads
* [02_detect_and_redact_pii.py](notebooks/02_detect_and_redact_pii.py) - a Delta Live Table notebook that uses the expectations and actions defined in the same JSON file [here](expectations/pii_detection.json) to detect and redact PII
* [03_tag_pii.py](notebooks/03_tag_pii.py) - a notebook that tags the generated databases/tables/columns based on the PII detected and actions taken
* [04_cleanup.py](notebooks/04_cleanup.py) - a notebook that can be used to clean up the input, storage and output data so that it can be re-tested

When everything is set up correctly you should see something like this...

## 1. Multi step job to automatically detect, redact and tag PII:



## 2. DLT pipeline to automatically detect and redact PII:




## 4. Example of the column-level PII tagging applied:



## 5. Query of the redacted output table:



