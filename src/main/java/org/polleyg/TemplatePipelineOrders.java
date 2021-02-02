package org.polleyg;

import com.google.api.services.bigquery.model.Row;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.schemas.*;
import com.google.cloud.spanner.Struct;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.fromTableSchema;
import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

public class TemplatePipelineOrders {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TemplateOptions.class);

        String project = "dkt-us-data-lake-a1xq";
        String dataset = "dkt_us_test_cap5000";
        String table = "orders";

        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pcollection1 = pipeline.apply("READ", TextIO.read().from(options.getInputFile()));
        //PCollection<String> pcollection1 = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-25_11_2020_21_36_25.json"));
        pcollection1.apply("TRANSFORM", ParDo.of(new TransformJsonParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:dkt_us_test_cap5000.orders", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrder()));


        pipeline.run();
    }

    private static TableSchema getTableSchemaOrder() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("customer_id").setType("INTEGER").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("street1").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("street2").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("zip_code").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("city").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("country").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("created_at").setType("DATETIME").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    private static TableSchema getTableSchemaOrderStatus() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("order_number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider.RuntimeValueProvider<String> getInputFile();

        void setInputFile(ValueProvider.RuntimeValueProvider<String> value);
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
            else {
                map.put("street2", "null");
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


    public static class TransformJsonParDo2 extends DoFn<String, TableRow> {

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
            Map<Object, Object> mapShipmentOrder = new HashMap<>();
            mapShipmentOrder.put("source","shopify");
            mapShipmentOrder.put("order_number",jsonObject.get("name"));
            mapShipmentOrder.put("updated_at", timeStampNow);

            for (Object o : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) o;
                mapShipmentOrder.put("id", fulfillment.get("name"));
                mapShipmentOrder.put("status", fulfillment.get("shipment_status"));

            }

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);
            System.out.println(listTableRow);

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
                TableFieldSchema col = getTableSchemaOrder().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }
public static class DeduplicateData extends DoFn<String, String> {
    public static void  main(String[] args) {
        // args[0] = tableName
        List<String> bigQueryTableList = Arrays.asList("customers", "order_errors", "order_items", "order_shipments", "order_sources", "order_status", "orders", "shipment_trackings");
        List<Pair<String, String>> uniqueBigQueryCombinaison = new ArrayList<Pair<String, String>>();
        uniqueBigQueryCombinaison.add(Pair.of("id", "lastname")); // customers table
        uniqueBigQueryCombinaison.add(Pair.of("order_number", "error_type")); // order_errors table
        uniqueBigQueryCombinaison.add(Pair.of("shipment_id", "source")); // order_items table
        uniqueBigQueryCombinaison.add(Pair.of("id", "source")); // order_shipments table
        uniqueBigQueryCombinaison.add(Pair.of("order_number", "source")); // order_sources table
        uniqueBigQueryCombinaison.add(Pair.of("order_number", "source")); // order_status table
        uniqueBigQueryCombinaison.add(Pair.of("number", "created_at")); // orders table
        uniqueBigQueryCombinaison.add(Pair.of("shipment_id", "source")); // shipment_trackings table

        String projectId = "MY_PROJECT_ID";
        String datasetName = "MY_DATASET_NAME";

        for (int i = 0; i < bigQueryTableList.size(); i++) {
            String query = "DELETE FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000." + bigQueryTableList.get(i) + "` d "
                    + "WHERE EXISTS (WITH redundant AS ( "
                    + "SELECT " + uniqueBigQueryCombinaison.get(i).getLeft() + "," + uniqueBigQueryCombinaison.get(i).getRight() + ", "
                    + "COUNT(*) AS counter FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000." + bigQueryTableList.get(i) + "` "
                    + "GROUP BY " + uniqueBigQueryCombinaison.get(i).getLeft() + "," + uniqueBigQueryCombinaison.get(i).getRight() + " HAVING counter > 1) "
                    + "SELECT 1 FROM redundant WHERE d." + uniqueBigQueryCombinaison.get(i).getLeft() + "=" + uniqueBigQueryCombinaison.get(i).getLeft() + " AND d." + uniqueBigQueryCombinaison.get(i).getRight() + "=" + uniqueBigQueryCombinaison.get(i).getRight() + ")";

            query(query);
        }
    }

    public static void query(String query) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult results = bigquery.query(queryConfig);

            results
                    .iterateAll()
                    .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

            System.out.println("Query performed successfully.");
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query not performed \n" + e.toString());
        }
    }
}
}
