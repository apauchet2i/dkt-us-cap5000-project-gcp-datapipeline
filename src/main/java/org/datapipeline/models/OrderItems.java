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

    public static class TransformJsonParDoOrderItemsShopifyList extends DoFn<String, List<TableRow>> {

        @ProcessElement
        public void mapJsonToBigQueryTableShopifyList(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray fulfillmentArray = (JSONArray) jsonObject.get("fulfillments");
            Map<Object, Object> mapOrderItems = new HashMap<>();

            for (Object fulfillments : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) fulfillments;
                JSONArray itemsArray = (JSONArray) fulfillment.get("line_items");
                for (Object items : itemsArray) {
                    JSONObject item = (JSONObject) items;
                    mapOrderItems.put("id", item.get("sku"));
                    mapOrderItems.put("shipment_id", fulfillment.get("name"));
                    mapOrderItems.put("source", "shopify");
                    mapOrderItems.put("name", item.get("name"));
                    mapOrderItems.put("price", item.get("price"));
                    mapOrderItems.put("quantity", item.get("quantity"));
                    mapOrderItems.put("updated_at", DateNow.dateNow());
                    JSONObject mapOrderItemsToBigQuery = new JSONObject(mapOrderItems);
                    TableRow tableRowOrderItems = convertJsonToTableRow(String.valueOf(mapOrderItemsToBigQuery));
                    listTableRow.add(tableRowOrderItems);
                }
            }
                c.output(listTableRow);
        }
    }

    public static class TransformJsonParDoOrderItemsShopify extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigQueryTableShopify(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray fulfillmentArray = (JSONArray) jsonObject.get("fulfillments");
            Map<Object, Object> mapOrderItems = new HashMap<>();

            for (Object fulfillments : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) fulfillments;
                JSONArray itemsArray = (JSONArray) fulfillment.get("line_items");
                for (Object items : itemsArray) {
                    JSONObject item = (JSONObject) items;
                    mapOrderItems.put("id", item.get("sku"));
                    mapOrderItems.put("shipment_id", fulfillment.get("name"));
                    mapOrderItems.put("source", "shopify");
                    mapOrderItems.put("name", item.get("name"));
                    mapOrderItems.put("price", item.get("price"));
                    mapOrderItems.put("quantity", item.get("quantity"));
                    mapOrderItems.put("updated_at", DateNow.dateNow());
                    JSONObject mapOrderItemsToBigQuery = new JSONObject(mapOrderItems);
                    TableRow tableRowOrderItems = convertJsonToTableRow(String.valueOf(mapOrderItemsToBigQuery));
                    listTableRow.add(tableRowOrderItems);
                }
            }
            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class TransformJsonParDoOrderItemsNewStore extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigQueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray orderedProductsArray = (JSONArray) jsonObject.get("ordered_products");
            Map<Object, Object> mapOrderItems = new HashMap<>();

            for (Object orderedProducts : orderedProductsArray) {
                JSONObject orderedProduct = (JSONObject) orderedProducts;

                mapOrderItems.put("id", orderedProduct.get("sku"));
                mapOrderItems.put("shipment_id", jsonObject.get("shipment_uuid"));
                mapOrderItems.put("source", "newstore");
                mapOrderItems.put("name", orderedProduct.get("name"));
                mapOrderItems.put("price", orderedProduct.get("price"));
                mapOrderItems.put("quantity", orderedProduct.get("quantity"));
                mapOrderItems.put("updated_at", DateNow.dateNow());
                JSONObject mapOrderItemsToBigQuery = new JSONObject(mapOrderItems);
                TableRow tableRowOrderItems = convertJsonToTableRow(String.valueOf(mapOrderItemsToBigQuery));
                listTableRow.add(tableRowOrderItems);
            }

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }

    public static class TransformJsonParDoOrderItemsShiphawk extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigQueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray shipmentLineItemsArray = (JSONArray) jsonObject.get("shipment_line_items");
            Map<Object, Object> mapOrderItems = new HashMap<>();

            for (Object shipmentLineItems : shipmentLineItemsArray) {
                JSONObject shipmentLineItem = (JSONObject) shipmentLineItems;
                mapOrderItems.put("id", shipmentLineItem.get("sku"));
                mapOrderItems.put("name", shipmentLineItem.get("name"));
                mapOrderItems.put("price", shipmentLineItem.get("value"));
                String shipmentIdWithPoint = (jsonObject.get("order_number").toString()).replace("-",".");
                mapOrderItems.put("shipment_id", shipmentIdWithPoint);
                mapOrderItems.put("source", "shiphawk");
                mapOrderItems.put("quantity", shipmentLineItem.get("quantity"));
                mapOrderItems.put("updated_at", DateNow.dateNow());
                JSONObject mapOrderItemsToBigQuery = new JSONObject(mapOrderItems);
                TableRow tableRowOrderItems = convertJsonToTableRow(String.valueOf(mapOrderItemsToBigQuery));
                listTableRow.add(tableRowOrderItems);
                }
            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }
}

