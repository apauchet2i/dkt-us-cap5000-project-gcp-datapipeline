package org.polleyg.object;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

public class ShipmentTrackings {

    public static TableSchema getTableSchemaShipmentTrackings() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("shipment_id").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("tracking_id").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("tracking_link").setType("STRING").setMode("NULLABLE"));
        return new TableSchema().setFields(fields);
    }

    public static class TransformJsonParDoShipmentTrackings extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray fulfillmentArray = (JSONArray) jsonObject.get("fulfillments");
            Map<Object, Object> mapShipmentOrder = new HashMap<>();
            mapShipmentOrder.put("source","shopify");

            for (Object o : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) o;
                mapShipmentOrder.put("shipment_id", fulfillment.get("name"));
                JSONArray trackingNumbers = (JSONArray) fulfillment.get("tracking_numbers");
                JSONArray trackingUrls = (JSONArray) fulfillment.get("tracking_urls");
                if (fulfillment.get("tracking_numbers") != null && trackingNumbers.size() != 0 ) {
                    mapShipmentOrder.put("tracking_id", trackingNumbers.get(0));
                } else {
                    mapShipmentOrder.put("tracking_id", "null");
                }
                if (fulfillment.get("tracking_urls") != null && trackingUrls.size()!= 0) {
                    mapShipmentOrder.put("tracking_link", trackingUrls.get(0));
                } else {
                    mapShipmentOrder.put("tracking_link", "null");
                }
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
