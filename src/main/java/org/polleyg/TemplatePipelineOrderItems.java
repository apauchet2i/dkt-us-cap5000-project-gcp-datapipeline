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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

public class TemplatePipelineOrderItems {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("READ", TextIO.read().from(options.getInputFile()))
                //pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-25_11_2020_21_36_25.json"))

                .apply("TRANSFORM", ParDo.of(new TransformJsonParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:dkt_us_test_cap5000.order_items", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderStatus()));
        pipeline.run();
    }

    private static TableSchema getTableSchemaOrderStatus() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("shipment_id").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("price").setType("NUMERIC").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("quantity").setType("INTEGER").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }

    public static class TransformJsonParDo extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            System.out.println(c.element().getClass());
            System.out.println(c.element());
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
            LocalDateTime now = LocalDateTime.now();
            String timeStampNow = dtf.format(now);

            JSONArray fulfillmentArray = (JSONArray) jsonObject.get("fulfillments");
            Map<Object, Object> mapOrderItems = new HashMap<>();

            for (Object fulfillments : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) fulfillments;
                JSONArray itemsArray = (JSONArray) fulfillment.get("line_items");
                for (Object items : itemsArray) {
                    JSONObject item = (JSONObject) items ;
                    mapOrderItems.put("id", item.get("sku"));
                    mapOrderItems.put("shipment_id", fulfillment.get("name"));
                    mapOrderItems.put("source", "shopify");
                    mapOrderItems.put("name", item.get("name"));
                    mapOrderItems.put("price", item.get("price"));
                    System.out.println(item.get("price"));
                    System.out.println(item.get("quantity"));
                    mapOrderItems.put("quantity", item.get("quantity"));
                    mapOrderItems.put("updated_at", timeStampNow);
                    JSONObject mapOrderItemsToBigQuery = new JSONObject(mapOrderItems);
                    TableRow tableRowOrderItems = convertJsonToTableRow(String.valueOf(mapOrderItemsToBigQuery));
                    listTableRow.add(tableRowOrderItems);
                    System.out.println(listTableRow);
                }
            }

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
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
                TableFieldSchema col = getTableSchemaOrderStatus().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }
}
