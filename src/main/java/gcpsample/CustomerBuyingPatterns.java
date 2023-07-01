package gcpsample;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CustomerBuyingPatterns {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerBuyingPatterns.class);
    public static void main(String[] args) {
        try {
            LOG.info("Job Started !!!!!!!!!!!!!!!!!!!!1");
            System.out.println("Job Started !!!!!!!!!!!!!!!!!!!!1");
            PipelineOptions options = PipelineOptionsFactory.create();
            options.as(GcpOptions.class).setProject(GCP_Constants.PROJECT_ID);
            //options.setRunner(DataflowRunner.class);
            options.setRunner(DirectRunner.class);
            options.setTempLocation("gs://sample_bucket_all/dataflowwrite/");


            Pipeline p = Pipeline.create(options);

            PCollection<TableRow> rawSalesData = p.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                    .from(GCP_Constants.PROJECT_ID + ":" + GCP_Constants.DATASET_ID + ".dvcl"));

            PCollection<TableRow> resultRows = rawSalesData.apply(ParDo.of(new DoFn<TableRow, TableRow>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    TableRow row = c.element();
                    String customerId = (String) row.get("customer_id");
                    String productId = (String) row.get("product_id");
                    Double revenue = (Double) row.get("revenue");
                    if (customerId != null && productId != null && revenue != null) {
                        TableRow resultRow = new TableRow();
                        resultRow.set("customer_id", customerId);
                        resultRow.set("product_id", productId);
                        resultRow.set("total_spent", revenue);
                        c.output(resultRow);
                    }
                }
            }));


            resultRows.apply(BigQueryIO.writeTableRows()
                    .to(GCP_Constants.PROJECT_ID + ":" + GCP_Constants.DATASET_ID + ".customer_buying_patterns")
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
            p.run().waitUntilFinish();
            LOG.info("Job Completed !!!!!!!!!!!!!!!!!!!!1");
            System.out.println("Job Completed !!!!!!!!!!!!!!!!!!!!1");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
