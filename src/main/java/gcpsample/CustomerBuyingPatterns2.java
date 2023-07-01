package gcpsample;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class CustomerBuyingPatterns2 {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerBuyingPatterns2.class);

    static class ExtractAndCleanDataFn extends DoFn<TableRow, KV<String, Long>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            try {
                String customerId = (String) row.get("customer_id");
                Long unitsSold = Long.parseLong((String) row.get("units_sold"));
                c.output(KV.of(customerId, unitsSold));
            } catch (NumberFormatException e) {
                LOG.error("Error occurred inside ExtractAndCleanDataFn !!!");
                LOG.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.as(GcpOptions.class).setProject(GCP_Constants.PROJECT_ID);
        //options.setRunner(DataflowRunner.class);
        options.setRunner(DirectRunner.class);
        options.setTempLocation("gs://sample_bucket_all/dataflowwrite/");
        Pipeline p = Pipeline.create(options);

        PCollection<TableRow> rawLines = p.apply("ReadFromBigQuery",
                BigQueryIO.readTableRows().from(GCP_Constants.PROJECT_ID+":"+GCP_Constants.DATASET_ID+".dvcl"));

        PCollection<KV<String, Long>> customerPurchases = rawLines
                .apply("ExtractAndCleanData", ParDo.of(new ExtractAndCleanDataFn()))
                .apply("CountPurchases", Count.perKey());

        PCollection<TableRow> output = customerPurchases.apply("FormatAsTableRow", ParDo.of(
                new DoFn<KV<String, Long>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Processing row: " + c.element());
                        c.output(new TableRow()
                                //.set("customer_id", c.element().getKey())
                                //.set("total_purchases", c.element().getValue())
                                .set("customer_id", Integer.valueOf(1))
                                .set("total_purchases", Integer.valueOf(10))
                                .set("total_quantity",Integer.valueOf(0))
                                .set("total_spent",Integer.valueOf(0))
                                .set("last_purchase_date", new Date())
                        );
                    }
                }));
        // Apply the custom coder to the PCollection
        //output.setCoder(NullableCoder.of(String.class));

        output.apply(BigQueryIO.writeTableRows()
                .to(GCP_Constants.PROJECT_ID+":"+GCP_Constants.DATASET_ID+".customer_buying_patterns")
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        p.run().waitUntilFinish();
    }
}

