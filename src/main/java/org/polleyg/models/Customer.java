package org.polleyg.models;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

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

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
            LocalDateTime now = LocalDateTime.now();
            String timeStampNow = dtf.format(now);

            JSONObject customer = (JSONObject) jsonObject.get("customer");

            Map<String, Object> mapCustomer = new HashMap<>();
            mapCustomer.put("id", String.valueOf(customer.get("id")));
            mapCustomer.put("lastname", String.valueOf(customer.get("last_name")));
            mapCustomer.put("firstname", String.valueOf(customer.get("first_name")));
            mapCustomer.put("updated_at", timeStampNow);

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

            JSONObject customer = (JSONObject) jsonObject.get("customer");
            if (customer.get("id") == null || customer.get("last_name") == null || customer.get("first_name") == null) {
                Map<String, Object> mapCustomerError = new HashMap<>();
                mapCustomerError.put("order_number", jsonObject.get("name"));
                mapCustomerError.put("error_type", "missing_customer_info");
                mapCustomerError.put("source", "shopify");

                JSONObject mapCustomerToBigQuery = new JSONObject(mapCustomerError);
                TableRow tableRow = convertJsonToTableRow(String.valueOf(mapCustomerToBigQuery));
                c.output(tableRow);
            }
        }
    }
}
