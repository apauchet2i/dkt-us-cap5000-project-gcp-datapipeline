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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.polleyg.utils.JsonToTableRow.convertJsonToTableRow;

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

    public static class TransformJsonParDoOrderStatus extends DoFn<String, TableRow> {

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

            System.out.println(listTableRow);

            for (TableRow tableRow : listTableRow) {
                c.output(tableRow);
            }
        }
    }
}
