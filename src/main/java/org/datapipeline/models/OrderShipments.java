package org.datapipeline.models;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.datapipeline.utils.DateNow;

import java.util.*;

import static org.datapipeline.utils.JsonToTableRow.convertJsonToTableRow;

public class OrderShipments {

    public static TableSchema getTableSchemaOrderShipments() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("order_number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public static class TransformJsonParDoOrderShipmentsShopifyList extends DoFn<String, List<TableRow>> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();

            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;
            JSONObject order = (JSONObject) jsonObject.get("order");


            JSONArray fulfillmentArray = (JSONArray) order.get("fulfillments");
            Map<Object, Object> mapShipmentOrder = new HashMap<>();
            mapShipmentOrder.put("source","shopify");
            mapShipmentOrder.put("order_number",order.get("name"));
            mapShipmentOrder.put("updated_at", DateNow.dateNow());

            for (Object o : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) o;
                mapShipmentOrder.put("id", fulfillment.get("name"));
                mapShipmentOrder.put("status", fulfillment.get("shipment_status"));
            }

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);

            c.output(listTableRow);
        }
    }

    public static class TransformJsonParDoOrderShipmentsShopify extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();

            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;
            JSONObject order = (JSONObject) jsonObject.get("order");

            JSONArray fulfillmentArray = (JSONArray) order.get("fulfillments");

            if (fulfillmentArray != null && fulfillmentArray.size() > 0 ) {
                Map<Object, Object> mapShipmentOrder = new HashMap<>();
                mapShipmentOrder.put("source", "shopify");
                mapShipmentOrder.put("order_number", order.get("name"));
                mapShipmentOrder.put("updated_at", DateNow.dateNow());

                for (Object o : fulfillmentArray) {
                    JSONObject fulfillment = (JSONObject) o;
                    mapShipmentOrder.put("id", fulfillment.get("name"));
                    mapShipmentOrder.put("status", fulfillment.get("shipment_status"));
                }

                JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
                TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
                listTableRow.add(tableRowStatusFulfillment);

                for (TableRow tableRow : listTableRow) {
                    c.output(tableRow);
                }
            }
        }
    }
    public static class TransformJsonParDoOrderShipmentsNewStore extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();

            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;



            JSONArray shipmentsArray = (JSONArray) jsonObject.get("shipments");
            Map<Object, Object> mapShipmentOrder = new HashMap<>();

            for (Object o : shipmentsArray) {
                JSONObject shipment = (JSONObject) o;
                mapShipmentOrder.put("order_number",jsonObject.get("external_order_id"));
                mapShipmentOrder.put("updated_at", DateNow.dateNow());
               // mapShipmentOrder.put("id", shipment.get("id")); // blocage value
                mapShipmentOrder.put("id", "id value ?");
                mapShipmentOrder.put("source","newstore");
                JSONObject details = (JSONObject) shipment.get("details");
                mapShipmentOrder.put("status", details.get("delivery_status_id"));
            }

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class TransformJsonParDoOrderShipmentsShiphawk extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();

            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            Map<Object, Object> mapShipmentOrder = new HashMap<>();
            mapShipmentOrder.put("id", jsonObject.get("order_number"));
            mapShipmentOrder.put("source","shiphawk");
            String orderNumber = jsonObject.get("order_number").toString();
            String[] splitOrderNumber = orderNumber.split("-");
            mapShipmentOrder.put("order_number",splitOrderNumber[0]);
            mapShipmentOrder.put("status", jsonObject.get("status"));
            mapShipmentOrder.put("updated_at", DateNow.dateNow());

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class TransformJsonParDoOrderShipmentsShiphawkList extends DoFn<String, List<TableRow>> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();

            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            Map<Object, Object> mapShipmentOrder = new HashMap<>();
            mapShipmentOrder.put("id", jsonObject.get("order_number"));
            mapShipmentOrder.put("source","shiphawk");
            String orderNumber = jsonObject.get("order_number").toString();
            String[] splitOrderNumber = orderNumber.split("-");
            mapShipmentOrder.put("order_number",splitOrderNumber[0]);
            mapShipmentOrder.put("status", jsonObject.get("status"));
            mapShipmentOrder.put("updated_at", DateNow.dateNow());

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);

            c.output(listTableRow);

        }
    }

    public static class mapOrderShipmentsError extends DoFn<TableRow, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element());
            if(c.element().get("status") == null || c.element().get("status").toString().equals("failure")) {
                TableRow TableRowOrderShipmentsError = new TableRow();
                TableRowOrderShipmentsError.set("order_number", c.element().get("order_number"));
                TableRowOrderShipmentsError.set("error_type", "delivery_failure");
                TableRowOrderShipmentsError.set("source", "shopify");
                TableRowOrderShipmentsError.put("updated_at", DateNow.dateNow());
                c.output(TableRowOrderShipmentsError);
            }
        }
    }

    public static class mapOrderShipmentsErrorNewStore extends DoFn<TableRow, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element());
            if(c.element().get("status") == null || c.element().get("status").toString().equals("failed") || c.element().get("status").toString().equals("rejected") || c.element().get("status").toString().equals("partially_rejected")) {
                TableRow TableRowOrderShipmentsError = new TableRow();
                TableRowOrderShipmentsError.set("order_number", c.element().get("order_number"));
                TableRowOrderShipmentsError.set("error_type", "delivery_failure");
                TableRowOrderShipmentsError.set("source", "shopify");
                TableRowOrderShipmentsError.put("updated_at", DateNow.dateNow());
                c.output(TableRowOrderShipmentsError);
            }
        }
    }

    public static class mapOrderShipmentsErrorShipHawk extends DoFn<TableRow, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element());
            if(c.element().get("status") == null) {
                TableRow TableRowOrderShipmentsError = new TableRow();
                TableRowOrderShipmentsError.set("order_number", c.element().get("order_number"));
                TableRowOrderShipmentsError.set("error_type", "delivery_failure");
                TableRowOrderShipmentsError.set("source", "shiphawk");
                TableRowOrderShipmentsError.set("updated_at", DateNow.dateNow());
                c.output(TableRowOrderShipmentsError);
            }
        }
    }
}
