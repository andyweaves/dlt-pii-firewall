# DLT PII Firewall

> :warning: **THIS PROJECT IS PROVIDED AS-IS WITHOUT ANY GUARANTEES**: we make no claims as to the accuracy of the PII detection provided here, and if you use it it is **YOUR RESPONSIBILITY** to ensure that the example regexes and detection/redaction/tagging meets your internal, legal or regulatory requirements.

## Using [Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) to identify and redact potential PII data!

[Delta Live Tables](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake.

## Setup

To get this pipeline running on your environment, please use the following steps:

1. First you need some input data to run the pipeline on. If you don't have any data, or just want to try it out, you can use the [00_generate_data.py](notebooks/00_generate_data.py) notebook to generate some, using the following options to customise that data generation:
   * ```GENERATE_CLEAN_DATA```: Whether to generate 4 records of artificially created "clean data" specifically designed not to get evaluated as PII
   * ```GENERATE_PII_DATA```: Whether to generate fake PII data
   * ```NUM_ROWS```: The number of rows of fake PII data to generate
   * ```OUTPUT_DIR```: The path on DBFS or cloud storage to write the generated data out to
3. Clone this Github Repo using our Repos for Git Integration (see the docs for [AWS](https://docs.databricks.com/repos/index.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/repos/), [GCP](https://docs.gcp.databricks.com/repos/index.html)). 
4. Create a new DLT pipeline, selecting [01_observability.py](notebooks/01_observability.py) and [02_detect_and_redact_pii.py](notebooks/02_detect_and_redact_pii.py) as Notebook Libraries (see the docs for [AWS](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html), [Azure](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-ui), [GCP](https://docs.gcp.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html)). You’ll need add the following Configuration:
   * ```INPUT_PATH```: The path on DBFS or cloud storage where the input data is located. Right now the code is expecting to find parquet files at this path, to change this to a different type of file just modify the code that defines the ```staging``` view in [02_detect_and_redact_pii.py](notebooks/02_detect_and_redact_pii.py):
     ```
     import dlt

      @dlt.view(
        name="staging",
        comment="Raw data that has not been scanned for PII"
      )
      def staging():
        return (
          spark.read.parquet(INPUT_PATH)
        )
      ```
   * ```TABLE_PATH```: The path to write out all of the tables created by the pipeline to.
   * ```STORAGE_PATH```: A location on DBFS or cloud storage where output data and metadata required for the pipeline execution are stored. This should match the ```Storage Location``` entered below.
   * ```EXPECTATIONS_PATH```: The path to the [pii_firewall_rules.json](expectations/pii_firewall_rules.json) config file once you've checked out the Repo. This is the main configuration file used to customise the behaviour of the detection/redaction/tagging of data. See **Firewall Rules** below for more details
   * ```Target```: The name of a database for persisting pipeline output data. Configuring the target setting allows you to view and query the pipeline output data from the Databricks UI.
   * ```Storage Location```: A location on DBFS or cloud storage where output data and metadata required for the pipeline execution are stored. This should match the ```STORAGE_PATH``` entered above.
5. Note: once you’ve edited the settings that are configurable via the UI, you’ll need to edit the JSON so that you can add the configuration needed to authenticate with your chosen cloud storage:
   * For AWS add the ```instance_profile_arn``` to the aws_attributes object.
   * For Azure add the Service Principal secrets to the ```spark_conf``` object.
   * For GCP add the ```google_service_account``` to the  ```gcp_attributes``` object.
6. As well as the DLT pipeline, the project contains the notebook [03_tag_pii.py](notebooks/03_tag_pii.py). This is designed to run after the DLT pipeline has finished, and tag databases/tables/columns appropriate to confirm that:
   * They have been scanned for PII
   * That PII has either been found or not found
   * Where PII has been found, add a customisable comment to the column it was found in
7. In order to get this to run straight after our DLT pipeline, we're going to create a multi task Job (see the docs for [AWS](https://docs.databricks.com/data-engineering/jobs/index.html), [Azure](https://docs.microsoft.com/en-gb/azure/databricks/data-engineering/jobs/), [GCP](https://docs.gcp.databricks.com/data-engineering/jobs/index.html). You'll need to select the notebook [03_tag_pii.py](notebooks/03_tag_pii.py) and pass in the following Parameters:
   * ```DATABASE_NAME```: The database to apply tagging (via properties) to. Should match the ```Target``` entered above.
   * ```TABLE_NAMES```: The tables within the databse to apply tagging (via properties) to and column level comments to. The DLT pipeline creates 3 main tables: ```clean```, ```redacted``` and ```clean_processed``` you can apply tagging to 1, 2 or all 3 of these.
   * ```EXPECTATIONS_PATH```: The path to the [pii_firewall_rules.json](expectations/pii_firewall_rules.json) config file once you've checked out the Repo. This is the main configuration file used to customise the behaviour of the detection/redaction/tagging of data. See **Firewall Rules** below for more details.

## Run the Job

When everything is set up correctly, run the MT Job and you should see something like this...

### 1. Multi-step job to automatically detect, redact and tag PII:

![image](https://user-images.githubusercontent.com/43955924/160146555-376fd977-cd91-4cd7-919a-f6eaa84df73c.png)

### 2. DLT pipeline to automatically detect and redact PII:

The pipeline following a successful run:

![image](https://user-images.githubusercontent.com/43955924/160136979-a16fc3c8-1fbe-4e0f-8660-24b4e8f52c0e.png)

The expectations evaluated against our sample data:

![image](https://user-images.githubusercontent.com/43955924/160137248-386e649e-d1a8-4c24-adeb-46bf734d7fad.png)

### 3. Example of the column level PII tagging applied:

![image](https://user-images.githubusercontent.com/43955924/160141168-07688e9e-b02c-4712-947f-3ddd79173942.png)

### 4. Example of the redacted output table:

![image](https://user-images.githubusercontent.com/43955924/160144577-84870f68-9460-45ed-b732-0865ac8cc63e.png)

## Output Tables



## Firewall Rules

The [pii_firewall_rules.json](expectations/pii_firewall_rules.json) file is the main way that you can customise the behaviour of how the detection/redaction/tagging of data works. Within the file you'll notice a number of rules defined as follows:

```
"name": "", 
"constraint": "",
"action": "",
"mode": "",
"tag":""
```
Every rule that you specify here will be evaluated as against every column of your input data. 

## Next Steps

There are some items of PII in the generated data that intentionally fail to get picked up fully by the expectations:

  * ```name```: It's impossible to match names via Regex, particularly when multiple languages need to be considered. This field was left in intentionally to demonstrate that there are limitations to the Regex approach.
  * ```freetext```: Expecations evaluate against each row, but once a row fails for a given column, any other expectations are not considered (the row has failed for that column, move on. Even if expectations worked differently, evaluating each expectation against every row for every column multiple times would be hugely expensive. Right now what this means is that the first PII detected in a field which has multiple types of PII will be flagged as the reason that column failed, and the redaction step for that PII element is the only that will be applied. Again, this field was left in intentionally to demonstrate that there are limitations with this approach.

In order to handle PII like this we'll need to use more sophisticated techniques like Named Entity Recognition and NLP. Stay tuned for more exploration on this topic!
