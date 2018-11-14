import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.LOG;


public class Pipeline_PubSub_BigQuery_output {

    // My own "Pair" class, in order to have (int,str) pairs
    private static class Pair<Integer, String> implements Serializable {
        private final Integer left;
        private final String right;

        Pair(Integer l, String r) {
            left = l;
            right = r;
        }

        Integer getLeft() {
            return left;
        }

        String getRight() {
            return right;
        }

        @Override
        public int hashCode() {
            return left.hashCode() ^ right.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Pair)) return false;
            Pair pairo = (Pair) o;
            return this.left.equals(pairo.getLeft()) &&
                    this.right.equals(pairo.getRight());
        }
    }

    // Functions to log message outputs
    private static class LogLowMarkMessages extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("The customer was \"" + c.element() + "\" :(");
            c.output(c.element());
        }
    }

    private static class LogHighMarkMessages extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("The customer was \"" + c.element() + "\" :)");
            c.output(c.element());
        }
    }

    // Functions to convert the data to TableRow
    private static class FormatDataFn implements SerializableFunction<String, TableRow> {
        public TableRow apply(String s) {
            return new TableRow().set("mark", s);
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

        // Definition of the side input; a bounded BigQuery source regading from a table with 2 columns (mark, satisfaction)(int, str)
        PCollection<KV<Integer, String>> sideInput = p.apply("Read mapping from BigQuery",
                BigQueryIO.read(new SerializableFunction<SchemaAndRecord, KV<Integer, String>>() {
                    public KV<Integer, String> apply(SchemaAndRecord input) {
                        GenericRecord record = input.getRecord();

                        // Return a KV with the result
                        return KV.of(((Long) record.get("mark")).intValue(), record.get("satisfaction").toString());
                    }
                }).from(gcp_res.getBq_input()));

        // Present the side input source as a View, so that it can be used as a Side Input in a ParDo
        final PCollectionView<Map<Integer, String>> sideInputView = sideInput.apply("SideInput to View", View.<Integer, String>asMap());


        /* SINKS */
        // Definition of the output tags for the multiple outputs
        final TupleTag<String> messagesOneToThree = new TupleTag<String>() {
        };
        final TupleTag<String> messagesFourToFive = new TupleTag<String>() {
        };


        /* MAIN PIPELINE OPERATIONS */
        PCollectionTuple output = mainInput
                // Apply a ParDo to the main input, passing a sideInput source too
                .apply("Combine Inputs", ParDo
                        .of(new DoFn<String, Pair<Integer, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Map<Integer, String> mapping = c.sideInput(sideInputView);

                                Integer fig = Integer.parseInt(c.element());
                                String num = mapping.get(Integer.parseInt(c.element()));

                                Pair<Integer, String> out = new Pair<Integer, String>(fig, num);

                                c.output(out);
                            }
                        }).withSideInputs(sideInputView))
                // Apply a function to split elements into multiple outputs
                .apply("Split outputs", ParDo.of(new DoFn<Pair<Integer, String>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, MultiOutputReceiver out) {
                        List<Integer> OneToThree = Arrays.asList(1, 2, 3);
                        List<Integer> FourToFive = Arrays.asList(4, 5);

                        if (OneToThree.contains(c.element().getLeft())) {
                            out.get(messagesOneToThree).output(c.element().getRight());
                        } else if (FourToFive.contains(c.element().getLeft())) {
                            out.get(messagesFourToFive).output(c.element().getRight());
                        }
                    }
                }).withOutputTags(messagesOneToThree, TupleTagList.of(messagesFourToFive)));

        // Apply more transforms to each of the outputs
        output.get(messagesOneToThree).apply("Log bad marks", ParDo.of(new LogLowMarkMessages()))
                .apply(BigQueryIO.<String>write()
                        .to(gcp_res.getBq_output_bad())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withFormatFunction(new FormatDataFn()));

        output.get(messagesFourToFive).apply("Log good marks", ParDo.of(new LogHighMarkMessages()))
                .apply(BigQueryIO.<String>write()
                        .to(gcp_res.getBq_output_good())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withFormatFunction(new FormatDataFn()));


        /* RUN THE PIPELINE */
        p.run();
    }
}