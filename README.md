# DLT PII Firewall

> :warning: **THIS PROJECT IS PROVIDED AS-IS WITHOUT ANY GUARANTEES**: we make no claims as to the accuracy of the PII detection provided here, and if you use it it is **YOUR RESPONSIBILITY** to ensure that the example regexes and detection/redaction/tagging meets your internal, legal or regulatory requirements.

### Using [Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) to identify and redact potential PII data!

[Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake.

### Setup

To get this pipeline running on your environment, please use the following steps:

1. First you need some input data to run the pipeline on. If you don't have any data, or just want to try it out, you can use the [00_generate_data.py](notebooks/00_generate_data.py) notebook to generate some, using the following options to customise that data generation:
   * ```GENERATE_CLEAN_DATA```: Whether to generate 4 records of artificially created "clean data" specifically designed not to get evaluated as PII
   * ```GENERATE_PII_DATA```: Whether to generate fake PII data
   * ```NUM_ROWS```: The number of rows of fake PII data to generate
   * ```OUTPUT_DIR```: The path on DBFS or cloud storage to write the generated data out to
3. Clone this Github Repo using our Repos for Git Integration (see the docs for [AWS](https://docs.databricks.com/repos/index.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/repos/), [GCP](https://docs.gcp.databricks.com/repos/index.html)). 
4. Create a new DLT pipeline, selecting [01_observability.py](notebooks/01_observability.py) and [02_detect_and_redact_pii.py](notebooks/02_detect_and_redact_pii.py) as Notebook Libraries (see the docs for [AWS](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-ui), [GCP](https://docs.gcp.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html)). You’ll need add the following Configuration:
   * ```INPUT_PATH```: The path on DBFS or cloud storage where the input data is located. Right now the code is expecting to find parquet files at this path
   * ```TABLE_PATH```: The path to write out all of the tables created by the pipeline to.
   * ```STORAGE_PATH```: A location on DBFS or cloud storage where output data and metadata required for the pipeline execution are stored. This should match the ```Storage Location``` below.
   * ```EXPECTATIONS_PATH```: The path to (pii_firewall_rules.json)[expectations/pii_firewall_rules.json] once you've checked out the Repo. This is the main configuration file used to customise the behaviour of the detection/redaction/tagging of data evaluated as PII.
   * ```Target```: The name of a database for persisting pipeline output data. Configuring the target setting allows you to view and query the pipeline output data from the Databricks UI.
   * ```Storage Location```: A location on DBFS or cloud storage where output data and metadata required for the pipeline execution are stored. This should match the ```STORAGE_PATH``` above.
5. Note: once you’ve edited the settings that are configurable via the UI, you’ll need to edit the JSON so that you can add the configuration needed to authenticate with your chosen cloud storage:
   * For AWS add the ```instance_profile_arn``` to the aws_attributes object.
   * For Azure add the Service Principal secrets to the ```spark_conf``` object.
   * For GCP add the ```google_service_account``` to the  ```gcp_attributes``` object.




### Run the Job

When everything is set up correctly you should see something like this...

#### 1. Multi-step job to automatically detect, redact and tag PII:

#### 2. DLT pipeline to automatically detect and redact PII:

The pipeline following a successful run:

![image](https://user-images.githubusercontent.com/43955924/160136979-a16fc3c8-1fbe-4e0f-8660-24b4e8f52c0e.png)

The expectations evaluated against our sample data:

![image](https://user-images.githubusercontent.com/43955924/160137248-386e649e-d1a8-4c24-adeb-46bf734d7fad.png)

#### 3. Example of the column level PII tagging applied:

![image](https://user-images.githubusercontent.com/43955924/160141168-07688e9e-b02c-4712-947f-3ddd79173942.png)

#### 4. Example of the redacted output table:

![image](https://user-images.githubusercontent.com/43955924/160144577-84870f68-9460-45ed-b732-0865ac8cc63e.png)

#### Next Steps:

There are some items of PII in the generated data that intentionally fail to get picked up fully by the expectations:

  * ```name```:
  * ```freetext```:

In order to handle PII like this we'll need to use more sophisticated techniques like Named Entity Recognition and NLP. Stay tuned for more exploration on this topic!
