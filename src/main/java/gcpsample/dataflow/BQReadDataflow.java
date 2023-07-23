package gcpsample.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import gcpsample.GCPConstants;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


public class BQReadDataflow {


    static class PrintFn extends DoFn<TableRow, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
        }
    }

    public static void main(String[] args) {
        System.out.println("@@@@@@@@@@ BQReadDataflow main started @@@@@@@@@@");
        PipelineOptions options = PipelineOptionsFactory.create();
        options.as(GcpOptions.class).setProject(GCPConstants.PROJECT_ID);
        options.setRunner(DataflowRunner.class);
        options.setTempLocation("gs://"+GCPConstants.DATAFLOW_GCS_BUCKET_NAME+"/"+GCPConstants.DATAFLOW_TEMP_FOLDER);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                        .from(GCPConstants.PROJECT_ID+":"+GCPConstants.DATASET_ID+"."+GCPConstants.GBQ_SOURCE_TABLE_NAME)
                .withoutValidation())
                .apply("Print", ParDo.of(new PrintFn()));

        pipeline.run().waitUntilFinish();
    }
}

