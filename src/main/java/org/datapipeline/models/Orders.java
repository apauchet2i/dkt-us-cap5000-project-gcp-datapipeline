package org.datapipeline.models;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.datapipeline.utils.DateNow;
import java.util.*;

import static org.datapipeline.utils.JsonToTableRow.convertJsonToTableRow;

public class Orders {

    public static TableSchema getTableSchemaOrder() {
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

    public static class TransformJsonParDoOrders extends DoFn<String, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {

            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;


            JSONObject order = (JSONObject) jsonObject.get("order");
            JSONObject customer = (JSONObject) order.get("customer");
            JSONObject shippingAddress = (JSONObject) order.get("shipping_address");

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
            map.put("created_at", ((String) order.get("created_at")).substring(0, ((String) order.get("created_at")).length() - 6));
            map.put("updated_at", DateNow.dateNow());
            JSONObject jsonToBigQuery = new JSONObject(map);
            TableRow tableRow = convertJsonToTableRow(String.valueOf(jsonToBigQuery));
            c.output(tableRow);
        }
    }
}
