package org.polleyg.object;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

/**
 * Do some randomness
 */
public class Customer {

    private static TableSchema getTableSchemaCustomer() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("lastname").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("firstname").setType("STRING").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public static class TransformJsonParDoCustomer extends DoFn<String, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            JSONParser parser = new JSONParser();
            System.out.println(c.element().getClass());
            System.out.println(c.element());
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONObject customer = (JSONObject) jsonObject.get("customer");

            Map<String, Object> mapCustomer = new HashMap<>();
            mapCustomer.put("id", String.valueOf(customer.get("id")));
            mapCustomer.put("lastname", String.valueOf(customer.get("last_name")));
            mapCustomer.put("firstname", String.valueOf(customer.get("first_name")));

            JSONObject mapCustomerToBigQuery = new JSONObject(mapCustomer);
            System.out.println(mapCustomerToBigQuery);
            TableRow tableRow = convertJsonToTableRow(String.valueOf(mapCustomerToBigQuery));
            System.out.println(tableRow);
            c.output(tableRow);
        }
    }
}
