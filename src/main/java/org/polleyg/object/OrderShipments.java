package org.polleyg.object;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

/**
 * Do some randomness
 */
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

    public static class TransformJsonParDoOrderShipments extends DoFn<String, TableRow> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List<TableRow> listTableRow = new ArrayList<>();
            JSONParser parser = new JSONParser();
            System.out.println(c.element().getClass());
            System.out.println(c.element());
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
            LocalDateTime now = LocalDateTime.now();
            String timeStampNow = dtf.format(now);

            JSONArray fulfillmentArray = (JSONArray) jsonObject.get("fulfillments");
            Map<Object, Object> mapShipmentOrder = new HashMap<>();
            mapShipmentOrder.put("source","shopify");
            mapShipmentOrder.put("order_number",jsonObject.get("name"));
            mapShipmentOrder.put("updated_at", timeStampNow);

            for (Object o : fulfillmentArray) {
                JSONObject fulfillment = (JSONObject) o;
                mapShipmentOrder.put("id", fulfillment.get("name"));
                mapShipmentOrder.put("status", fulfillment.get("shipment_status"));

            }

            JSONObject mapShipmentOrderToBigQuery = new JSONObject(mapShipmentOrder);
            TableRow tableRowStatusFulfillment = convertJsonToTableRow(String.valueOf(mapShipmentOrderToBigQuery));
            listTableRow.add(tableRowStatusFulfillment);
            System.out.println(listTableRow);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }
}
