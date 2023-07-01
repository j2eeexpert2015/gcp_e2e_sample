package gcpsample;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.runners.direct.DirectRunner;


public class BQRead {


    static class PrintFn extends DoFn<TableRow, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
        }
    }

    public static void main(String[] args) {
        //PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        PipelineOptions options = PipelineOptionsFactory.create();
        options.as(GcpOptions.class).setProject(GCP_Constants.PROJECT_ID);
        //options.setRunner(DataflowRunner.class);
        options.setRunner(DirectRunner.class);
        options.setTempLocation("gs://sample_bucket_all/dataflow/");
        //options.setStagingLocation("gs://my-bucket/staging/");
        //options.setTempLocation("gs://my-bucket/temp/");
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                        .from(GCP_Constants.PROJECT_ID+":"+GCP_Constants.DATASET_ID+".dvcl")
                .withoutValidation())
                .apply("Print", ParDo.of(new PrintFn()));

        pipeline.run().waitUntilFinish();
    }
}

