package gcpsample;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.options.PipelineOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GBQDataFlowJob {
    private static final Logger LOG = LoggerFactory.getLogger(GBQDataFlowJob.class);
    public static void main(String[] args) {

        // Create a PipelineOptions object. This object lets us set various execution options
        // for our pipeline, such as the runner you wish to use.
        PipelineOptions options = PipelineOptionsFactory.create();
        options.as(GcpOptions.class).setProject(GCP_Constants.PROJECT_ID);
        options.setRunner(DirectRunner.class);
        options.setTempLocation("gs://sample_bucket_all/dataflowwrite/");

        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);

        // Define the BigQuery source table
        String sourceTable = "poised-shuttle-384406:gcpsample.source_table";

        // Define the BigQuery destination table
        String destinationTable = "poised-shuttle-384406:gcpsample.target_table";

        // Read data from the BigQuery source table
        pipeline.apply("ReadFromBQ",
                        BigQueryIO.readTableRows()
                                .from(sourceTable))
                .apply("LogOutput", ParDo.of(new DoFn<TableRow, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRow row = c.element();
                        // Log the data read
                        LOG.info("Data: " + row.toString());
                        c.output(row);
                    }
                }))
                // Write the data to the BigQuery target table
                .apply("WriteToBQ",
                        BigQueryIO.writeTableRows()
                                .to(destinationTable)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
