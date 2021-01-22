package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.json.*;

import java.io.ByteArrayInputStream;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
        //pipeline.apply("READ", TextIO.read().from(options.getInputFile()))
        pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-05_01_2021_10_11_36.json"))
                .apply(
                        "JSONtoData",                     // the transform name
                        ParDo.of(new DoFn<String, TableRow>() {    // a DoFn as an anonymous inner class instance
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                JSONParser parser = new JSONParser();
                                System.out.println(c.element().getClass());
                                System.out.println(c.element());
                                try {
                                    Object obj = parser.parse(c.element());
                                    JSONObject jsonObject = (JSONObject) obj;
                                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("Y-m-d'T'H:MM:SS", Locale.getDefault());
                                    LocalDateTime now = LocalDateTime.now();
                                    String timeStampNow = dtf.format(now);


                                    JSONObject customer = (JSONObject) jsonObject.get("customer");
                                    JSONObject shippingAddress = (JSONObject) jsonObject.get("shipping_address");
                                    System.out.println(customer.get("id"));
                                    System.out.println(customer.get("id").getClass());

                                    Map<String, String> map = new HashMap<>();
                                    map.put("number", (String) jsonObject.get("name"));
                                    map.put("customer_id", String.valueOf(customer.get("id")));
                                    map.put("street1", (String) shippingAddress.get("address1"));
                                    map.put("street2", (String) shippingAddress.get("address2"));
                                    map.put("zip_code", (String) shippingAddress.get("zip"));
                                    map.put("city", (String) shippingAddress.get("city"));
                                    map.put("country", (String) shippingAddress.get("country"));
                                    map.put("created_at", ((String) jsonObject.get("created_at")).substring(0,((String) jsonObject.get("created_at")).length()-6));
                                    map.put("updated_at", timeStampNow);
                                    JSONObject jsonToBigQuery = new JSONObject(map);
                                    System.out.println(jsonToBigQuery);
                                    c.output(convertJsonToTableRow(String.valueOf(jsonToBigQuery)));

                                }catch (Exception e) {
                                    e.printStackTrace();
                                }

                           }
                        }))
                //.apply("TRANSFORM", ParDo.of(new WikiParDo()));
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

    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
                     new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }

        return row;
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }

    public static class WikiParDo extends DoFn<String, TableRow> {
        public static final String HEADER = "number,customer_id,street1,street2,zip_code,city,country,created_at,updated_at";

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            if (c.element().equalsIgnoreCase(HEADER)) return;
            String[] split = c.element().split(",");
            if (split.length > 9) return;
            TableRow row = new TableRow();
            for (int i = 0; i < split.length; i++) {
                TableFieldSchema col = getTableSchemaOrder().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }
}
