# Cloud Dataflow
This repository contains a small set of [Cloud Dataflow](https://cloud.google.com/dataflow/) samples. Dataflow is the GCP managed service for stream and batch data processing, that makes use of [Apache Beam SDK](https://beam.apache.org/) over the GCP infrastructure.

Up to now, this repository consists of 4 samples (each of which is an extension of the previous one) that explain the step-by-step process to build a Streaming Pipeline (using the Java Apache Beam SDK) that reads messages from [Pub/Sub](https://cloud.google.com/pubsub/) (the publisher-subscriber GCP product) and writes some output to [BigQuery](https://cloud.google.com/bigquery/) (a well-known serverless data warehouse GCP service). Some modifications are being applied in each of the samples so that the complexity of the pipeline increases, ending up with a code that reads both from a Main Input and a [Side Input](https://beam.apache.org/documentation/programming-guide/#side-inputs), and also writes to [multiple destinations](https://beam.apache.org/documentation/programming-guide/#additional-outputs).

## Run instructions
#### GCP project-specific resources
The helper class **GCP_Resources** just includes a list of the project-specific resources in your GCP Project (Pub/Sub subscription and BigQuery tables). You should modify the placeholders in order to match your resources.

#### Pipeline execution
The streaming pipeline can be run locally (using Direct Runner) for testing purposes:
`mvn compile exec:java -Dexec.mainClass=[CLASS_TO_EXECUTE] -Dexec.args="--tempLocation=gs://[BUCKET]/tmp --project=[PROJECT_ID]"`

Or remotely, in the Dataflow service (with the Dataflow Runner):
`mvn compile exec:java -Dexec.mainClass=[CLASS_TO_EXECUTE] -Dexec.args="--jobName=[JOB_NAME] --tempLocation=gs://[BUCKET]/tmp --project=[PROJECT_ID] --runner=DataflowRunner" -Pdataflow-runner`

#### Pub/Sub message publication
You can use the [Google Cloud SDK](https://cloud.google.com/sdk/gcloud/reference/pubsub/topics/publish) in order to publish a message to Pub/Sub, like below, being `[TOPIC]` your Pub/Sub topic, and `[MESSAGE]` simply a figure representing a grade (*1* to *5*):
`gcloud pubsub topics publish [TOPIC] --message=[MESSAGE]`

#### BigQuery data
The BigQuery input table should look like:
```
| mark |     satisfaction      |
|    1 | Dissatisfied          |
|    2 | Somewhat dissatisfied |
|    3 | Neutral               |
|    4 | Somewhat happy        |
|    5 | Happy                 |
```
Additionally, for the complete sample, you will need to have two output tables (`high_mark` and `low_mark`) with a single string field (`mark`).

## Pipelines explanation
In order to fully understand the 4 samples and the reasoning for starting from a basic pipeline and expanding it with additional functionalities, the samples should be studied in the following order:
- **Pipeline_SideInputs**:
This is a very simple initial pipeline which reads from hardcoded main and side inputs. This is a batch pipeline because it reads from a bounded data source.
- **Pipeline_PubSub_HardcodedSideInput**:
The first expansion of the pipeline will read from a Pub/Sub subscription, which is an unbounded data source (thus making it an streaming pipeline by default). The messages read from the pipeline contain a single figure *[1, 5]* which represents the quality level reported by a user. Each of these values is then matched to a hardcoded side input that represents a mapping between the numeric value of the figure, and its written form. Finally, a transform logs out each of the input messages on its written form.
- **Pipeline_PubSub_BigQuery**:
Some minor modifications are done in order to change the side input source from a hardcoded PCollection, to a BigQuery source.
- **Pipeline_PubSub_BigQuery_output**:
First, some general improvements are applied to the pipeline, in order to adapt it to the future changes. Then, we add multiple outputs (as TupleTag), and divided our elements depending on the mark provided in the input message. Those elements are then treated separately and have their own logging transform, as well as different BigQuery sinks.