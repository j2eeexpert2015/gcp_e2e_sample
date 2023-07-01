package gcpsample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;


public class CustomerBuyingPatterns {

    static class ExtractAndCleanDataFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");
            try {
                String customerId = fields[0];
                Integer unitsSold = Integer.parseInt(fields[6]); // Assuming units sold is at index 6
                c.output(KV.of(customerId, unitsSold));
            } catch (NumberFormatException e) {
                // Handle or log the exception
            }
        }
    }

    static class FormatAsTableRowFn extends DoFn<KV<String, Long>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(new TableRow().set("customer_id", c.element().getKey())
                    .set("total_purchases", c.element().getValue()));
        }
    }

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> rawLines = p.apply("ReadLines", TextIO.read().from("gs://bucket-name/input-files-location/*"));

        PCollection<KV<String, Long>> customerPurchases = rawLines
                .apply("ExtractAndCleanData", ParDo.of(new ExtractAndCleanDataFn()))
                .apply("CountPurchases", Count.perKey());

        PCollection<TableRow> tableRows = customerPurchases.apply("FormatAsTableRow", ParDo.of(new FormatAsTableRowFn()))
                .setCoder(TableRowJsonCoder.of());

        tableRows.apply(BigQueryIO.writeTableRows()
                .to("projectid:datasetid.customer_buying_patterns")
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run().waitUntilFinish();
    }
}
