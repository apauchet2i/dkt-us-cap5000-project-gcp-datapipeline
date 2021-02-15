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

public class ShipmentTrackings {

    public static TableSchema getTableSchemaShipmentTrackings() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("shipment_id").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("tracking_id").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("tracking_link").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public static class TransformJsonParDoShipmentTrackingsShopifyList extends DoFn<String, List<TableRow>> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray fulfillmentArray = (JSONArray) jsonObject.get("fulfillments");

            if (fulfillmentArray != null && fulfillmentArray.size() > 0 ) {

                Map<Object, Object> mapShipmentOrder = new HashMap<>();
                mapShipmentOrder.put("source", "shopify");

                for (Object o : fulfillmentArray) {
                    JSONObject fulfillment = (JSONObject) o;
                    mapShipmentOrder.put("shipment_id", fulfillment.get("name"));
                    JSONArray trackingNumbers = (JSONArray) fulfillment.get("tracking_numbers");
                    JSONArray trackingUrls = (JSONArray) fulfillment.get("tracking_urls");
                    if (fulfillment.get("tracking_numbers") != null && trackingNumbers.size() != 0) {
                        mapShipmentOrder.put("tracking_id", trackingNumbers.get(0));
                    } else {
                        mapShipmentOrder.put("tracking_id", "null");
                    }
                    if (fulfillment.get("tracking_urls") != null && trackingUrls.size() != 0) {
                        mapShipmentOrder.put("tracking_link", trackingUrls.get(0));
                    } else {
                        mapShipmentOrder.put("tracking_link", "null");
                    }
                    mapShipmentOrder.put("updated_at", DateNow.dateNow());
                }

                JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
                TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
                listTableRow.add(tableRowStatusFulfillment);
                c.output(listTableRow);

            }
            else {
                c.output((listTableRow));
            }
        }
    }

    public static class TransformJsonParDoShipmentTrackingsShopify extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;
            JSONObject order = (JSONObject) jsonObject.get("order");

            JSONArray fulfillmentArray = (JSONArray) order.get("fulfillments");
            Map<Object, Object> mapShipmentOrder = new HashMap<>();
            mapShipmentOrder.put("source", "shopify");

            for (Object o : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) o;
                mapShipmentOrder.put("shipment_id", fulfillment.get("name"));
                JSONArray trackingNumbers = (JSONArray) fulfillment.get("tracking_numbers");
                JSONArray trackingUrls = (JSONArray) fulfillment.get("tracking_urls");
                if (fulfillment.get("tracking_numbers") != null && trackingNumbers.size() != 0) {
                    mapShipmentOrder.put("tracking_id", trackingNumbers.get(0));
                } else {
                    mapShipmentOrder.put("tracking_id", "null");
                }
                if (fulfillment.get("tracking_urls") != null && trackingUrls.size() != 0) {
                    mapShipmentOrder.put("tracking_link", trackingUrls.get(0));
                } else {
                    mapShipmentOrder.put("tracking_link", "null");
                }
                mapShipmentOrder.put("updated_at", DateNow.dateNow());
            }

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class TransformJsonParDoShipmentTrackingsShipHawk extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray packageArray = (JSONArray) jsonObject.get("packages");
            Map<Object, Object> mapShipmentOrder = new HashMap<>();

            for (Object o : packageArray) {
                JSONObject packages = (JSONObject) o;
                mapShipmentOrder.put("shipment_id", jsonObject.get("order_number"));
                mapShipmentOrder.put("source", "shiphawk");
                mapShipmentOrder.put("tracking_id", packages.get("tracking_number"));
                mapShipmentOrder.put("tracking_link", packages.get("tracking_url"));
                mapShipmentOrder.put("updated_at", DateNow.dateNow());
            }

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class mapShipmentTrackingErrorShopify extends DoFn<String, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;
            JSONObject order = (JSONObject) jsonObject.get("order");
            JSONArray fulfillmentArray = (JSONArray) order.get("fulfillments");

            for (Object o : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) o;
                JSONArray trackingNumbers = (JSONArray) fulfillment.get("tracking_numbers");

                if (fulfillment.get("tracking_numbers") == null && trackingNumbers.size() == 0) {
                    TableRow TableRowOrderStatusError = new TableRow();
                    TableRowOrderStatusError.set("order_number", order.get("name"));
                    TableRowOrderStatusError.set("error_type", "tracking_number_error");
                    TableRowOrderStatusError.set("source", "shopify");
                    TableRowOrderStatusError.set("updated_at", DateNow.dateNow());
                    c.output(TableRowOrderStatusError);
                    listTableRow.add(TableRowOrderStatusError);
                }
            }
            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class mapShipmentTrackingErrorShiphawk extends DoFn<String, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray packageArray = (JSONArray) jsonObject.get("packages");
            Map<Object, Object> mapShipmentOrder = new HashMap<>();

            int nbTrackingNumber = 0;
            for (Object o : packageArray) {
                JSONObject packages = (JSONObject) o;
                mapShipmentOrder.put("shipment_id", jsonObject.get("order_number"));
                mapShipmentOrder.put("source", "shiphawk");
                if(packages.get("tracking_number") != null && !packages.get("tracking_number").equals("")){
                    nbTrackingNumber++;
                }
            }
            if (nbTrackingNumber == 0 ){
                TableRow TableRowOrderStatusError = new TableRow();
                String orderNumber = jsonObject.get("order_number").toString();
                String[] splitOrderNumber = orderNumber.split("-");
                TableRowOrderStatusError.set("order_number", splitOrderNumber[0]);
                TableRowOrderStatusError.set("error_type", "tracking_number_error");
                TableRowOrderStatusError.set("source", "shiphawk");
                TableRowOrderStatusError.set("updated_at", DateNow.dateNow());
                c.output(TableRowOrderStatusError);
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

