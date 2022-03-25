# dlt-pii-firewall
Using [Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) to identify and redact potential PII data!

> :warning: **This is a demo and is provided as-is**: we make no claims as to the accuracy of the PII detection provided here, and if you use it it is **YOUR** responsibility to ensure that the example regexes and detection/redaction/tagging meets your requirements and legal obligations.

The project is currently designed to be run as a [multi task job](https://docs.databricks.com/data-engineering/jobs/jobs-quickstart.html) on the world's favourite Lakehouse Platform... [Databricks](https://databricks.com/)!

In the [notebooks folder](notebooks/) you will find the following:

* [00_generate_data.py](notebooks/00_generate_data.py) - a notebook that can be used to generate fake PII data
* [01_observability.py](notebooks/01_observability.py) - a Delta Live Table notebook that creates an observability pipeline for our DLT workloads
* [02_detect_and_redact_pii.py](notebooks/02_detect_and_redact_pii.py) - a Delta Live Table notebook that uses the expectations and actions defined in the same JSON file [here](expectations/pii_detection.json) to detect and redact PII
* [03_tag_pii.py](notebooks/03_tag_pii.py) - a notebook that tags the generated databases/tables/columns based on the PII detected and actions taken
* [04_cleanup.py](notebooks/04_cleanup.py) - a notebook that can be used to clean up the input, storage and output data so that it can be re-tested

When everything is set up correctly you should see something like this...

## 1. Multi step job to automatically detect, redact and tag PII:

![image](https://user-images.githubusercontent.com/43955924/152864772-662c8697-a858-4d4f-94b2-d12363f36f4d.png)

## 2. DLT pipeline to automatically detect and redact PII:

![image](https://user-images.githubusercontent.com/43955924/152864961-732fb2ca-58dc-4805-96fa-110b3a186adc.png)

## 3. Example of the column-level PII tagging applied:

![image](https://user-images.githubusercontent.com/43955924/152865773-de1f6f53-2c66-4f8c-b6fc-0d8d0358ef97.png)

## 4. Query of the redacted output table:

![image](https://user-images.githubusercontent.com/43955924/152866375-0c91bcd1-6204-40a9-ab5d-f92cd2913f25.png)
