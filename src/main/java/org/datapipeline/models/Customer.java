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

public class Customer {

    public static TableSchema getTableSchemaCustomer() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("lastname").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("firstname").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public static class TransformJsonParDoCustomer extends DoFn<String, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONObject order = (JSONObject) jsonObject.get("order");
            JSONObject customer = (JSONObject) order.get("customer");

            Map<String, Object> mapCustomer = new HashMap<>();
            mapCustomer.put("id", String.valueOf(customer.get("id")));
            mapCustomer.put("lastname", String.valueOf(customer.get("last_name")));
            mapCustomer.put("firstname", String.valueOf(customer.get("first_name")));
            mapCustomer.put("updated_at", DateNow.dateNow());

            JSONObject mapCustomerToBigQuery = new JSONObject(mapCustomer);

            TableRow tableRow = convertJsonToTableRow(String.valueOf(mapCustomerToBigQuery));

            c.output(tableRow);
        }
    }

    public static class mapOrderCustomersError extends DoFn<String, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONObject order = (JSONObject) jsonObject.get("order");
            JSONObject customer = (JSONObject) order.get("customer");
            System.out.println(customer.get("id"));
            System.out.println(customer.get("last_name"));
            System.out.println(customer.get("first_name"));
            if (customer.get("id") == null || customer.get("last_name") == null || customer.get("first_name") == null) {
                Map<String, Object> mapCustomerError = new HashMap<>();
                mapCustomerError.put("order_number", jsonObject.get("name"));
                mapCustomerError.put("error_type", "missing_customer_info");
                mapCustomerError.put("source", "shopify");
                mapCustomerError.put("updated_at", DateNow.dateNow());

                JSONObject mapCustomerToBigQuery = new JSONObject(mapCustomerError);
                TableRow tableRow = convertJsonToTableRow(String.valueOf(mapCustomerToBigQuery));
                c.output(tableRow);
            }
        }
    }
}
