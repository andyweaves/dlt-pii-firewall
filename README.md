# dlt-pii-detection
A demo of using [Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) to identify and redact potential PII data!

> :warning: **This is a demo and is provided as-is**: we make no claims as to the accuracy of the PII detection provided here, and if you use it it is **YOUR** responsibility to ensure that the example regexes and detection/redaction/tagging pipelines meet your requirements.

The project is currently designed to be run as a [multi task job](https://docs.databricks.com/data-engineering/jobs/jobs-quickstart.html) on the world's favourite analytics platform... [Databricks](https://databricks.com/)!

In the [notebooks folder](notebooks/) you will find the following:

[00_generate_data.py](notebooks/00_generate_data.py) - a notebook that can be used to generate fake PII data
[01_identify_pii.py](notebooks/01_identify_pii.py) - a Delta Live Table notebook that uses a JSON file of [expectations](expectations/pii_detection.json) and their associated regexes to detect PII
[02_redact_pii.py](notebooks/02_redact_pii.py) - a Delta Live Table notebook that uses the actions defined in the same JSON file of [expectations](expectations/pii_detection.json) to redact columns that might contain PII
[03_tag_pii.py](notebooks/03_tag_pii.py) - a notebook that tags the generated databases/tables/columns based on the PII detected
[04_truncate_and_vacuum.py](notebooks/04_truncate_and_vacuum.py) - a notebook that truncates and then vacuums the quarantine table which is used to temporarily store any records containing PII that have been detected
[05_queries.sql](notebooks/05_queries.sql) - a notebook that can be used to query the output tables
[06_cleanup.py](notebooks/06_cleanup.py) - a notebook that can be used to clean up the input, storage and output data so that it can be re-tested

