package org.polleyg.object;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

public class OrderItems {

    public static TableSchema getTableSchemaOrderItems() {
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

    public static class TransformJsonParDoOrderItems extends DoFn<String, TableRow> {

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
}
