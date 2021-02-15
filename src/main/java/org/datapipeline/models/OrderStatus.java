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

public class OrderStatus {

    public static TableSchema getTableSchemaOrderStatus() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("order_number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("type").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public static class TransformJsonParDoOrderStatusShopifyList extends DoFn<String, List<TableRow>> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            Map<Object, Object> mapOrderStatus = new HashMap<>();
            mapOrderStatus.put("order_number", jsonObject.get("name"));
            mapOrderStatus.put("source", "shopify");
            mapOrderStatus.put("type", "order");
            if(jsonObject.get("cancelled_at") != null) {
                mapOrderStatus.put("status", "cancelled");
            }
            else if(jsonObject.get("closed_at") != null){
                mapOrderStatus.put("status", "closed");
            }
            else{
                mapOrderStatus.put("status", "opened");
            }
            mapOrderStatus.put("updated_at", DateNow.dateNow());
            JSONObject mapStatusOrderToBigQuery = new JSONObject(mapOrderStatus);
            TableRow tableRowStatusOrder = convertJsonToTableRow(String.valueOf(mapStatusOrderToBigQuery));

            listTableRow.add(tableRowStatusOrder);

            Map<Object, Object> mapFulfillmentStatus = new HashMap<>();
            mapFulfillmentStatus.put("order_number", jsonObject.get("name"));
            mapFulfillmentStatus.put("source", "shopify");
            mapFulfillmentStatus.put("type", "fulfillment");
            mapFulfillmentStatus.put("status", jsonObject.get("fulfillment_status"));
            mapFulfillmentStatus.put("updated_at", DateNow.dateNow());
            JSONObject mapStatusFulfillmentToBigQuery = new JSONObject(mapFulfillmentStatus);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapStatusFulfillmentToBigQuery));

            listTableRow.add(tableRowStatusFulfillment);

            Map<Object, Object> mapPaymentStatus = new HashMap<>();
            mapPaymentStatus.put("order_number", jsonObject.get("name"));
            mapPaymentStatus.put("source", "shopify");
            mapPaymentStatus.put("type", "payment");
            mapPaymentStatus.put("status", jsonObject.get("financial_status"));
            mapPaymentStatus.put("updated_at", DateNow.dateNow());
            JSONObject mapStatusPaymentToBigQuery = new JSONObject(mapPaymentStatus);
            TableRow tableRowStatusPayment = convertJsonToTableRow(String.valueOf(mapStatusPaymentToBigQuery));

            listTableRow.add(tableRowStatusPayment);
            c.output(listTableRow);
        }
    }

    public static class TransformJsonParDoOrderStatusShopify extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;
            JSONObject order = (JSONObject) jsonObject.get("order");

            Map<Object, Object> mapOrderStatus = new HashMap<>();
            mapOrderStatus.put("order_number", order.get("name"));
            mapOrderStatus.put("source", "shopify");
            mapOrderStatus.put("type", "order");
            if(order.get("cancelled_at") != null) {
                mapOrderStatus.put("status", "cancelled");
            }
            else if(order.get("closed_at") != null){
                mapOrderStatus.put("status", "closed");
            }
            else{
                mapOrderStatus.put("status", "opened");
            }
            mapOrderStatus.put("updated_at", DateNow.dateNow());
            JSONObject mapStatusOrderToBigQuery = new JSONObject(mapOrderStatus);
            TableRow tableRowStatusOrder = convertJsonToTableRow(String.valueOf(mapStatusOrderToBigQuery));

            listTableRow.add(tableRowStatusOrder);

            Map<Object, Object> mapFulfillmentStatus = new HashMap<>();
            if(order.get("fulfillment_status") == null){
                mapFulfillmentStatus.put("status", "unfulfilled");
            } else {
                mapFulfillmentStatus.put("status", order.get("fulfillment_status"));
            }
            mapFulfillmentStatus.put("order_number", order.get("name"));
            mapFulfillmentStatus.put("source", "shopify");
            mapFulfillmentStatus.put("type", "fulfillment");
            mapFulfillmentStatus.put("updated_at", DateNow.dateNow());
            JSONObject mapStatusFulfillmentToBigQuery = new JSONObject(mapFulfillmentStatus);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapStatusFulfillmentToBigQuery));

            listTableRow.add(tableRowStatusFulfillment);

            Map<Object, Object> mapPaymentStatus = new HashMap<>();
            mapPaymentStatus.put("order_number", order.get("name"));
            mapPaymentStatus.put("source", "shopify");
            mapPaymentStatus.put("type", "payment");
            mapPaymentStatus.put("status", order.get("financial_status"));
            mapPaymentStatus.put("updated_at", DateNow.dateNow());
            JSONObject mapStatusPaymentToBigQuery = new JSONObject(mapPaymentStatus);
            TableRow tableRowStatusPayment = convertJsonToTableRow(String.valueOf(mapStatusPaymentToBigQuery));

            listTableRow.add(tableRowStatusPayment);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }
    public static class TransformJsonParDoOrderStatusNewStore extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;


            Map<Object, Object> mapOrderStatus = new HashMap<>();
            mapOrderStatus.put("order_number", jsonObject.get("order_id"));
            mapOrderStatus.put("source", "newstore");
            mapOrderStatus.put("type", "order");
            mapOrderStatus.put("status", jsonObject.get("status_label"));
            mapOrderStatus.put("updated_at", DateNow.dateNow());
            JSONObject mapStatusOrderToBigQuery = new JSONObject(mapOrderStatus);
            TableRow tableRowStatusOrder = convertJsonToTableRow(String.valueOf(mapStatusOrderToBigQuery));

            listTableRow.add(tableRowStatusOrder);

            Map<Object, Object> mapPaymentStatus = new HashMap<>();
            mapPaymentStatus.put("order_number", jsonObject.get("order_id"));
            mapPaymentStatus.put("source", "newstore");
            mapPaymentStatus.put("type", "payment");
            mapPaymentStatus.put("updated_at", DateNow.dateNow());
            JSONArray paymentArray = (JSONArray) jsonObject.get("payments");
            for (Object o : paymentArray) {
                JSONObject payment = (JSONObject) o;
                mapPaymentStatus.put("status", payment.get("status_label").toString().toLowerCase());
            }

            JSONObject mapStatusPaymentToBigQuery = new JSONObject(mapPaymentStatus);
            TableRow tableRowStatusPayment = convertJsonToTableRow(String.valueOf(mapStatusPaymentToBigQuery));
            listTableRow.add(tableRowStatusPayment);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class mapOrderStatusError extends DoFn<TableRow, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element());
            System.out.println(c.element().get("type"));
            System.out.println(c.element().get("type").toString());
            if(c.element().get("type").toString().equals("payment") && c.element().get("status").toString().equals("voided")) {
                TableRow TableRowOrderStatusError = new TableRow();
                TableRowOrderStatusError.set("order_number", c.element().get("order_number"));
                TableRowOrderStatusError.set("error_type", "payment_failure");
                TableRowOrderStatusError.set("source", "shopify");
                TableRowOrderStatusError.set("updated_at", DateNow.dateNow());
                c.output(TableRowOrderStatusError);
            }
        }
    }

    public static class mapOrderStatusErrorNewStore extends DoFn<TableRow, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            if(c.element().get("type").toString().equals("payment") && c.element().get("status").toString().equals("pending")) {
                TableRow TableRowOrderStatusError = new TableRow();
                TableRowOrderStatusError.set("order_number", c.element().get("order_number"));
                TableRowOrderStatusError.set("error_type", "payment_failure");
                TableRowOrderStatusError.set("source", "newstore");
                TableRowOrderStatusError.set("updated_at", DateNow.dateNow());
                c.output(TableRowOrderStatusError);
            }
        }
    }
}
