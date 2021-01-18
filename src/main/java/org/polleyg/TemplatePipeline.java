package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

/**
 * Do some randomness
 */
public class TemplatePipeline {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("READ", TextIO.read().from(options.getInputFile()))
                .apply("TRANSFORM", ParDo.of(new WikiParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:dkt_us_test_cap5000.orders", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrder()));
        pipeline.run();
    }

    private static TableSchema getTableSchemaOrder() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("number").setType("STRING"));
        fields.add(new TableFieldSchema().setName("customer_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("street1").setType("STRING"));
        fields.add(new TableFieldSchema().setName("street2").setType("STRING"));
        fields.add(new TableFieldSchema().setName("zip_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("city").setType("STRING"));
        fields.add(new TableFieldSchema().setName("country").setType("STRING"));
        fields.add(new TableFieldSchema().setName("created_at").setType("DATETIME"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME"));
        return new TableSchema().setFields(fields);
    }


    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }

    public static class WikiParDo extends DoFn<String, TableRow> {
        public static final String HEADER = "number,customer_id,street1,street2,zip_code,city,country,created_at, updated_at";

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            if (c.element().equalsIgnoreCase(HEADER)) return;
            String[] split = c.element().split(",");
            if (split.length > 7) return;
            TableRow row = new TableRow();
            for (int i = 0; i < split.length; i++) {
                TableFieldSchema col = getTableSchemaOrder().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }
}
