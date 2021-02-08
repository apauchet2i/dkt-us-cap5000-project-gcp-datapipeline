package org.polleyg.models;

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

public class OrderStatus {

    static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
    static LocalDateTime now = LocalDateTime.now();
    static String timeStampNow = dtf.format(now);

    public static TableSchema getTableSchemaOrderStatus() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("order_number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("type").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public static class TransformJsonParDoOrderStatusShopify extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
            LocalDateTime now = LocalDateTime.now();
            String timeStampNow = dtf.format(now);

            Map<Object, Object> multimapStatusOrder = new HashMap<>();
            multimapStatusOrder.put("order_number", jsonObject.get("name"));
            multimapStatusOrder.put("source", "shopify");
            multimapStatusOrder.put("type", "order");
            if(jsonObject.get("cancelled_at") != null) {
                multimapStatusOrder.put("status", "cancelled");
            }
            else if(jsonObject.get("closed_at") != null){
                multimapStatusOrder.put("status", "closed");
            }
            else{
                multimapStatusOrder.put("status", "opened");
            }
            multimapStatusOrder.put("updated_at", timeStampNow);
            JSONObject multimapStatusOrderToBigQuery = new JSONObject(multimapStatusOrder);
            TableRow tableRowStatusOrder = convertJsonToTableRow(String.valueOf(multimapStatusOrderToBigQuery));

            listTableRow.add(tableRowStatusOrder);

            Map<Object, Object> multimapStatusFulfillment = new HashMap<>();
            multimapStatusFulfillment.put("order_number", jsonObject.get("name"));
            multimapStatusFulfillment.put("source", "shopify");
            multimapStatusFulfillment.put("type", "fulfillment");
            multimapStatusFulfillment.put("status", jsonObject.get("fulfillment_status"));
            multimapStatusFulfillment.put("updated_at", timeStampNow);
            JSONObject multimapStatusFulfillmentToBigQuery = new JSONObject(multimapStatusFulfillment);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(multimapStatusFulfillmentToBigQuery));

            listTableRow.add(tableRowStatusFulfillment);

            Map<Object, Object> multimapStatusPayment = new HashMap<>();
            multimapStatusPayment.put("order_number", jsonObject.get("name"));
            multimapStatusPayment.put("source", "shopify");
            multimapStatusPayment.put("type", "payment");
            multimapStatusPayment.put("status", jsonObject.get("financial_status"));
            multimapStatusPayment.put("updated_at", timeStampNow);
            JSONObject multimapStatusPaymentToBigQuery = new JSONObject(multimapStatusPayment);
            TableRow tableRowStatusPayment = convertJsonToTableRow(String.valueOf(multimapStatusPaymentToBigQuery));

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



            Map<Object, Object> multimapStatusOrder = new HashMap<>();
            multimapStatusOrder.put("order_number", jsonObject.get("order_id"));
            multimapStatusOrder.put("source", "newstore");
            multimapStatusOrder.put("type", "order");
            multimapStatusOrder.put("status", jsonObject.get("status_label"));
            multimapStatusOrder.put("updated_at", timeStampNow);
            JSONObject multimapStatusOrderToBigQuery = new JSONObject(multimapStatusOrder);
            TableRow tableRowStatusOrder = convertJsonToTableRow(String.valueOf(multimapStatusOrderToBigQuery));

            listTableRow.add(tableRowStatusOrder);

            Map<Object, Object> multimapStatusPayment = new HashMap<>();
            multimapStatusPayment.put("order_number", jsonObject.get("order_id"));
            multimapStatusPayment.put("source", "newstore");
            multimapStatusPayment.put("type", "payment");
            JSONArray paymentArray = (JSONArray) jsonObject.get("payments");
            for (Object o : paymentArray) {
                JSONObject payment = (JSONObject) o;
                multimapStatusPayment.put("status", payment.get("status_label").toString().toLowerCase());

            }
            multimapStatusPayment.put("updated_at", timeStampNow);
            JSONObject multimapStatusPaymentToBigQuery = new JSONObject(multimapStatusPayment);
            TableRow tableRowStatusPayment = convertJsonToTableRow(String.valueOf(multimapStatusPaymentToBigQuery));

            listTableRow.add(tableRowStatusPayment);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class mapOrderStatusError extends DoFn<TableRow, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            if(c.element().get("type").toString().equals("payment") && c.element().get("status").toString().equals("voided")) {
                TableRow TableRowOrderStatusError = new TableRow();
                TableRowOrderStatusError.set("order_number", c.element().get("order_number"));
                TableRowOrderStatusError.set("error_type", "payment_failure");
                TableRowOrderStatusError.set("source", "shopify");
                TableRowOrderStatusError.set("updated_at", timeStampNow);
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
                TableRowOrderStatusError.set("updated_at", timeStampNow);
                c.output(TableRowOrderStatusError);
            }
        }
    }
}
