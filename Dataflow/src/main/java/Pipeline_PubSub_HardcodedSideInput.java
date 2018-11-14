import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import java.util.HashMap;
import java.util.Map;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.LOG;


public class Pipeline_PubSub_HardcodedSideInput {

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

        // Definition of the side input; a hardcoded HashMap of the type (K, V)(int, str)
        Map<Integer, String> inputs2 = new HashMap<Integer, String>();
        inputs2.put(1, "one");
        inputs2.put(2, "two");
        inputs2.put(3, "three");
        // Create a PCollection from the Map
        PCollection<KV<Integer, String>> sideInput = p.apply("Create hardcoded SideInput", Create.of(inputs2));
        // Create a View from the Pcollection, so that it can be used as a Side Input in a ParDo
        final PCollectionView<Map<Integer, String>> sideInputView = sideInput.apply("SideInput to View", View.<Integer, String>asMap());


        /* MAIN PIPELINE OPERATIONS */
        mainInput
                .apply("Combine Inputs", ParDo
                .of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Map<Integer, String> mapping = c.sideInput(sideInputView);
                        String out = mapping.get(Integer.parseInt(c.element()));

                        c.output(out);
                    }
                }).withSideInputs(sideInputView))
        .apply("Log Messages", ParDo.of(new LogOutMessages()));


        /* RUN THE PIPELINE */
        p.run();
    }
}