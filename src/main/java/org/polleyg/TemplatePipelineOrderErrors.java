package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

public class TemplatePipelineOrderErrors {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.as(TemplateOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> pcollection = pipeline.apply("READ", TextIO.read().from(options.getInputFile()));
                //pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-05_01_2021_10_11_36.json"))

        pcollection.apply("TRANSFORM", ParDo.of(new TransformJsonParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:dkt_us_test_cap5000.order_errors", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_TRUNCATE)
                        .withSchema(getTableSchemaOrder()));

        pcollection.apply("TRANSFORM", ParDo.of(new TransformJsonParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:dkt_us_test_cap5000.order_errors", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_TRUNCATE)
                        .withSchema(getTableSchemaOrder()));
        pipeline.run();
    }

    private static TableSchema getTableSchemaOrder() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("order_number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("error_type").setType("STRING").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider.RuntimeValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }



    public static class TransformJsonParDo extends DoFn<String, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
            LocalDateTime now = LocalDateTime.now();
            String timeStampNow = dtf.format(now);

            JSONObject customer = (JSONObject) jsonObject.get("customer");
            JSONObject shippingAddress = (JSONObject) jsonObject.get("shipping_address");

            Map<String, Object> map = new HashMap<>();
            map.put("number", jsonObject.get("name"));
            map.put("customer_id", String.valueOf(customer.get("id")));
            map.put("street1", shippingAddress.get("address1"));
            if (shippingAddress.get("address2") != null) {
                map.put("street2", shippingAddress.get("address2"));
            }
            map.put("zip_code", shippingAddress.get("zip"));
            map.put("city", shippingAddress.get("city"));
            map.put("country", shippingAddress.get("country"));
            map.put("created_at", ((String) jsonObject.get("created_at")).substring(0, ((String) jsonObject.get("created_at")).length() - 6));
            map.put("updated_at", timeStampNow);
            JSONObject jsonToBigQuery = new JSONObject(map);
            TableRow tableRow = convertJsonToTableRow(String.valueOf(jsonToBigQuery));
            c.output(tableRow);
        }
    }

    public static class WikiParDo extends DoFn<String, TableRow> {
        public static final String HEADER = "year,month,day,wikimedia_project,language,title,views";

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
