import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import java.util.Map;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.LOG;


public class Pipeline_PubSub_BigQuery {

    // External function to log message output
    private static class LogOutMessages extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("This is the output: " + c.element());
            c.output(c.element());
        }
    }

    public static void main(String[] args) {

        /* Class where all the project-specific GCP resources are defined */
        GCP_Resources gcp_res = new GCP_Resources();

        /* PIPELINE CREATION */
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
        Pipeline p = Pipeline.create(options);


        /* SOURCES */
        // Definition of the main input; an unbounded Pub/Sub source reading from a subscription
        String subscription = gcp_res.getSubscription();
        PCollection<String> mainInput = p.apply("Read Messages from Pub/Sub", PubsubIO.readStrings().fromSubscription(subscription));

        // Definition of the side input; a bounded BigQuery source regading from a table with 2 columns (figure, number)(int, str)
        PCollection<KV<Integer, String>> sideInput = p.apply(
                BigQueryIO.read(new SerializableFunction<SchemaAndRecord, KV<Integer, String>>() {
                    public KV<Integer, String> apply(SchemaAndRecord input) {
                        GenericRecord record = input.getRecord();

                        // Return a KV with the result
                        return KV.of(((Long) record.get("mark")).intValue(), record.get("satisfaction").toString());
                    }
                }).from(gcp_res.getBq_input()));
        // Present the side input source as a View, so that it can be used as a Side Input in a ParDo
        final PCollectionView<Map<Integer, String>> sideInputView = sideInput.apply("SideInput to View", View.<Integer, String>asMap());


        /* MAIN PIPELINE OPERATIONS */
        mainInput
                // Apply a ParDo to the main input, passing a sideInput source too
                .apply("Combine Inputs", ParDo
                        .of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Map<Integer, String> mapping = c.sideInput(sideInputView);
                                String out = mapping.get(Integer.parseInt(c.element()));

                                c.output(out);
                            }
                        }).withSideInputs(sideInputView))
                // Apply a function to log message output
                .apply("Log Messages", ParDo.of(new LogOutMessages()));


        /* RUN THE PIPELINE */
        p.run();
    }
}