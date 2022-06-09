# DLT PII Firewall

> :warning: **THIS PROJECT IS PROVIDED AS-IS WITHOUT ANY GUARANTEES**: we make no claims as to the accuracy of the PII detection provided here, and if you decide to use it, it's **YOUR RESPONSIBILITY** to ensure that the example regexes and detection/redaction/tagging meets your internal, legal or regulatory requirements.

## Using [Delta Live Tables](https://databricks.com/product/delta-live-tables) to detect and redact PII data

![image](https://user-images.githubusercontent.com/43955924/172695949-0823d4eb-fe81-4f3e-aaf3-f6987a8c7ea7.png)

[Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake.

## Setup

To get this pipeline running on your environment, please use the following steps:

1. First you need some input data to run the pipeline on. If you don't have any data, or just want to try it out, you can use the [00_generate_data.py](notebooks/00_generate_data.py) notebook to generate some, using the following options to customise that data generation:
   * ```GENERATE_CLEAN_DATA```: Whether to generate 4 records of artificially created "clean data" specifically designed not to get evaluated as PII
   * ```GENERATE_PII_DATA```: Whether to generate fake PII data
   * ```NUM_ROWS```: The number of rows of fake PII data to generate
   * ```OUTPUT_DIR```: The path on cloud storage to write the generated data out to
3. Clone this Github Repo using Databricks Repos (see the docs for [AWS](https://docs.databricks.com/repos/index.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/repos/), [GCP](https://docs.gcp.databricks.com/repos/index.html)). 
4. Create a new DLT pipeline, selecting [01_observability.py](notebooks/01_observability.py) and [02_detect_and_redact_pii.py](notebooks/02_detect_and_redact_pii.py) as Notebook Libraries (see the docs for [AWS](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-ui), [GCP](https://docs.gcp.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html)). You’ll need add the following pipeline settings:
   * ```INPUT_PATH```: The path on cloud storage where the input data is located
   * ```INPUT_FORMAT```: The format of the input data. One of "delta", "parquet", "json", "csv" is supported
   * ```TABLE_PATH```: The path to write out all of the tables created by the pipeline to
   * ```STORAGE_PATH```: A location on cloud storage where output data and metadata required for the pipeline execution are stored. This should match the ```Storage Location``` entered below.
   * ```EXPECTATIONS_PATH```: The path to the [pii_firewall_rules.json](expectations/pii_firewall_rules.json) config file once you've checked out the Repo. This is the main configuration file used to customise the behaviour of the detection/redaction/tagging of data. See **Firewall Rules** below for more details
   * ```NUM_SAMPLE_ROWS```: In order to generate the SQL used to automatically redact the PII discovered, the pipeline will sample this many rows of data, evaluate it against your expectations and generate SQL code to leave it unchanged or redact it accordingly. The fewer rows sampled, the faster this initial stage of the pipeline will run, albeit the more likely that PII may make it through our firewall
   * ```FILL_NULLS```: If a row evaluates to ```NULL``` that will cause DLT expectations to fail, and you will see a high number of false positives. If your data is likely to contain many null values, set this to True and the pipeline will automatically fill them for you
   * ```Target```: The name of a database for persisting pipeline output data. Configuring the target setting allows you to view and query the pipeline output data from the Databricks UI
   * ```Storage Location```: A location on cloud storage where output data and metadata required for the pipeline execution are stored. This should match the ```STORAGE_PATH``` entered above.
5. Note: once you’ve edited the settings that are configurable via the UI, you’ll need to edit the JSON so that you can add the configuration needed to authenticate with your chosen cloud storage:
   * For AWS add the ```instance_profile_arn``` to the aws_attributes object.
   * For Azure add the Service Principal secrets to the ```spark_conf``` object.
   * For GCP add the ```google_service_account``` to the  ```gcp_attributes``` object.
6. As well as the DLT pipeline, the project contains the notebook [03_tag_pii.py](notebooks/03_tag_pii.py). This is designed to run after the DLT pipeline has finished, and tag databases/tables/columns appropriate to confirm that:
   * They have been scanned for PII
   * That PII has either been found or not found
   * Where PII has been found, add a customisable comment to the column it was found in
7. In order to get this to run straight after our DLT pipeline, we're going to create a multi-task Job Workflow (see the docs for [AWS](https://docs.databricks.com/data-engineering/jobs/index.html), [Azure](https://docs.microsoft.com/en-gb/azure/databricks/data-engineering/jobs/), [GCP](https://docs.gcp.databricks.com/data-engineering/jobs/index.html)). You'll need to select the notebook [03_tag_pii.py](notebooks/03_tag_pii.py) and pass in the following Parameters:
   * ```DATABASE_NAME```: The database to apply tagging (via properties) to. Should match the ```Target``` entered above.
   * ```TABLE_NAMES```: The tables within the databse to apply tagging (via properties) to and column level comments to. The DLT pipeline creates 3 main tables: ```clean```, ```redacted``` and ```output``` you can apply tagging to 1, 2 or all 3 of these.
   * ```EXPECTATIONS_PATH```: The path to the [dynamic_firewall_rules.json](expectations/dynamic_firewall_rules.json) config file once you've checked out the Repo. This is the main configuration file used to customise the behaviour of the detection/redaction/tagging of data. See **Firewall Rules** below for more details.

## Output Tables

The following data tables and views are created by this pipeline:

![image](https://user-images.githubusercontent.com/43955924/172695249-5ff5d38e-eda5-43fc-98c8-d0c32b18b109.png)

| Name            | Type  | Description         |
| ------------    | ----  | -----------         |
| staging         | View  | Initial view that data is load into. May contain PII and therefore declared as a view (so that PII is not persisted after the pipeline has been run |
| quarantine      | View  | View containing data that has failed expectations. May contain PII and therefore declared as a view (so that PII is not persisted after the pipeline has been run |
| clean           | Table | Table containing data that has passed expectations and therefore is not expected to contain PII  |
| redacted        | Table | Table containing data that has failed expectations and therefore is expected to contain PII but in which that PII has been redacted based on the specified actions |
| output | Table | A union of clean and redacted, creating a table that contains either data that has passed expectations and therefore is not expected to contain PII or data that is expected to contain PII but has been redacted based on the specified actions |

The following monitoring tables are created by this pipeline:

| Name                      | Type  | Description                                                                    |
| ------------              | ----  | -----------                                                                    |
| event_logs                | Table | Raw DLT event logs relating to the running and management of the pipeline      |
| audit_logs                | Table | Logs capturing the management events relating to the pipeline                  |
| data_quality_logs         | Table | Logs capturing the DQ metrics relating to the pipeline                         |
| flow_logs                 | Table | Logs capturing the runtime events relating to the pipeline                     |

## Run the Job

When everything is set up correctly, run the MT Job and you should see something like this...

### 1. Multi-task Job Workflow to automatically detect, redact and tag PII:

![image](https://user-images.githubusercontent.com/43955924/172706115-480d2f05-fdca-4d2c-bdd1-f67386edc6fe.png)

### 2. DLT pipeline to automatically detect and redact PII:

The pipeline following a successful run (10M rows of data):

![image](https://user-images.githubusercontent.com/43955924/172705228-0a3e6da1-6775-4a91-b91c-c9a75fc7c435.png)

The expectations evaluated against our sample data:

![image](https://user-images.githubusercontent.com/43955924/172705383-c6f28ee0-1c32-4d6c-9201-50ec8aaa9c87.png)

### 3. Example of the column level PII tagging applied:

![image](https://user-images.githubusercontent.com/43955924/172706693-e82515a1-ccad-4d3a-9f42-5c7178220c3f.png)

### 4. Example of the redacted output table:

![image](https://user-images.githubusercontent.com/43955924/172708036-ef22d79b-2330-4ccb-b562-dcb4b25e499d.png)

## Firewall Rules

The [dynamic_firewall_rules.json](expectations/dynamic_firewall_rules.json) file is the main way that you can customise the behaviour of how the detection/redaction/tagging of data works. Within the file you'll notice a number of rules defined as follows:

```
"name": "", 
"constraint": "",
"action": "",
"mode": "",
"tag":""
```
Every rule that you specify here will be applied against every column of your input data. To add new rules, just add a new JSON object as follows:

| Element    | Is Mandatory | Can Contain | Description |
|------------|-----------|---------|-------------|
| name       | Yes | any string | The name of the expectation. {} will be replaced by the column name.|
| constraint | Yes | a valid SQL invariant  | The expectation on which success or failure will determine whether the row contains PII or not. |
| action     | No  | a valid SQL expression | The action that will be applied if any rows in the column fail when evaluated against their expectation AND the mode selected is REDACT or REDACT_AND_TAG |
| mode       | Yes | One of "REDACT", "TAG", or "REDACT_AND_TAG" | Whether to REDACT, just TAG or REDACT_AND_TAG all of the rows in any column if any rows in the column fail when evaluated against their expectation  |
| tag        | No  | any string  | The comment to be added to any columns found to contain the. {} will be replaced by the column name. |

## Next Steps

* A firewall is only as good as its rules - the regular expressions provided are done so on an as-is basis and without any guarantees. That said, I’d love to test them against more data, refine their accuracy and add new regexes for PII types which aren’t covered yet. If you’re interested in collaborating on this, I’d love to hear from you!
* In my experience most of the enterprise platforms that I encounter in this field still rely on regexes for ~90% of their PII detection. That said, it’s important to acknowledge that this approach is not without its limitations. If you used the notebook provided to generate your input data you’ll notice that there are two fields that our detection has failed pretty miserably on:
  * ```name``` - short of having a dictionary of every person’s name on the planet - in every language and every alphabet - detecting a person’s name via regex is pretty much an impossible job
  * ```freetext``` - the way that expectations works means that if an expectation fails that row will not be evaluated against further expectations. It’s similar to error handling - if that row has failed, we mark it as failed and move on. The complication of this is that for fields that may contain multiple instances of PII (such as freetext fields), only the first type of PII identified will be identified and redacted.
  * So what’s the answer here? I’d like to look at using more sophisticated approaches like Named Entity Recognition (NER) or Natural Language Processing (NLP) to improve the detection of specific PII types like names, addresses or businesses, and also to apply identification and redaction to the contents of an entire column at a time. Again, if you’re interested in collaborating on this, please let me know!
* 80 - 90% of the world’s data is unstructured - whilst the approaches outlined here work well for structured data, unstructured data comes with its own, entirely different challenges! 
