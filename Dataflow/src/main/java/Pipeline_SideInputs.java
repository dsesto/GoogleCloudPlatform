import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.LOG;


public class Pipeline_SideInputs {

    // External function to log message output
    private static class LogOutMessages extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("This is the output: " + c.element());
            c.output(c.element());
        }
    }

    public static void main(String[] args) {

        /* PIPELINE CREATION */
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
        Pipeline p = Pipeline.create(options);


        /* SOURCES */
        // Definition of the main input; a list of integer values
        List<Integer> inputs1 = Arrays.asList(1, 2, 3, 2, 3, 1, 1);
        // Create a PCollection from the list
        PCollection<Integer> mainInput = p.apply("Create hardcoded MainInput", Create.of(inputs1));

        // Definition of the side input; a hardcoded HashMap of the type (K, V)(int, str)
        Map<Integer, String> inputs2 = new HashMap<Integer, String>();
        inputs2.put(1, "one");
        inputs2.put(2, "two");
        inputs2.put(3, "three");
        // Create a Pcollection from the Map
        PCollection<KV<Integer, String>> sideInput = p.apply("Create hardcoded SideInput", Create.of(inputs2));
        // Create a View from the Pcollection, so that it can be used as a Side Input in a ParDo
        final PCollectionView<Map<Integer, String>> sideInputView = sideInput.apply("SideInput to View", View.<Integer, String>asMap());


        /* MAIN PIPELINE OPERATIONS */
        mainInput
        .apply("Combine Inputs", ParDo
                .of(new DoFn<Integer, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Map<Integer, String> mapping = c.sideInput(sideInputView);
                        String out = mapping.get(c.element());

                        c.output(out);
                    }
                }).withSideInputs(sideInputView))
        .apply("Log Messages", ParDo.of(new LogOutMessages()));


        /* RUN THE PIPELINE */
        p.run();
    }
}