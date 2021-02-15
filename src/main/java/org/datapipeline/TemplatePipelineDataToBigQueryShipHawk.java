package org.datapipeline;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.datapipeline.models.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.datapipeline.models.OrderErrors.*;
import static org.datapipeline.models.OrderItems.*;
import static org.datapipeline.models.OrderShipments.*;
import static org.datapipeline.models.OrderSources.*;
import static org.datapipeline.models.ShipmentTrackings.*;

public class TemplatePipelineDataToBigQueryShipHawk {
    public static void main(String[] args) {

        String project = "dkt-us-data-lake-a1xq";
        String dataset = "dkt_us_test_cap5000";

        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pCollectionDataJson = pipeline.apply("READ DATA IN JSON FILE", TextIO.read().from(options.getInputFile()));

        // ********************************************   ORDER SHIPMENTS TABLE   ********************************************
        PCollection<List<TableRow>> rowsOrderShipmentsList = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SHIPMENTS", ParDo.of(new TransformJsonParDoOrderShipmentsShiphawkList()));
        PCollection<TableRow> rowsOrderShipments = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SHIPMENTS", ParDo.of(new TransformJsonParDoOrderShipmentsShiphawk()));
        rowsOrderShipments
                .apply("WRITE DATA IN BIGQUERY ORDER SHIPMENTS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_shipments", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderShipments()));
        rowsOrderShipmentsList
                .apply("FORMAT MESSAGE ORDER SHIPMENTS", ParDo.of(new TemplatePipelineDataToBigQueryShopify.CountMessageList("Order_shipments_pipeline_completed","order_shipments")))
                .apply("WRITE PUB MESSAGE ORDER SHIPMENTS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-order-shipments"));

        // ********************************************   ORDER SHIPMENTS ERROR    ********************************************
        PCollection<TableRow> rowsOrderShipmentsErrors = rowsOrderShipments.apply("TRANSFORM JSON TO TABLE ROW ERROR", ParDo.of(new mapOrderShipmentsErrorShipHawk()));
        rowsOrderShipmentsErrors
                .apply("WRITE DATA IN BIGQUERY ERRORS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_errors", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderErrors()));
        rowsOrderShipmentsErrors
                .apply("FORMAT MESSAGE ORDER ERRORS", ParDo.of(new TemplatePipelineDataToBigQueryShopify.CountMessage("Order_status-errors_pipeline_completed","order_error")))
                .apply("WRITE PUB MESSAGE ORDER ERRORS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-order-errors"));

        // ********************************************   ORDER ITEMS TABLE   ********************************************
        PCollection<List<TableRow>> rowsOrderItemsList = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER ITEMS", ParDo.of(new TransformJsonParDoOrderItemsShopifyList()));
        PCollection<TableRow> rowsOrderItems = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER ITEMS", ParDo.of(new TransformJsonParDoOrderItemsShiphawk()));
        rowsOrderItems
                .apply("WRITE DATA IN BIGQUERY ORDER ITEMS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_items", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderItems()));
        rowsOrderItemsList
                .apply("FORMAT MESSAGE ORDER ITEMS", ParDo.of(new TemplatePipelineDataToBigQueryShopify.CountMessageList("Order_items_pipeline_completed","order_items")))
                .apply("WRITE PUB MESSAGE ORDER ITEMS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-order-items"));

        // ********************************************   ORDER SOURCES TABLE   ********************************************
        PCollection<TableRow> rowsOrderSources = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SOURCES", ParDo.of(new OrderSources.TransformJsonParDoOrderSourcesShipHawk()));
        WriteResult writeResultOrderSources = rowsOrderSources.apply("WRITE DATA IN BIGQUERY ORDER SOURCES TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_sources", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderSources()));
        rowsOrderSources.apply(Wait.on(writeResultOrderSources.getFailedInserts()))
                .apply("COUNT MESSAGE", ParDo.of(new TemplatePipelineDataToBigQueryShopify.CountMessage("Order_sources_pipeline_completed","order_sources")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   SHIPMENT TRACKINGS TABLE   ********************************************
        PCollection<TableRow> rowsShipmentTrackings = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW SHIPMENT TRACKINGS", ParDo.of(new ShipmentTrackings.TransformJsonParDoShipmentTrackingsShipHawk()));
        WriteResult writeResultShipmentTrackings = rowsShipmentTrackings.apply("WRITE DATA IN BIGQUERY SHIPMENT TRACKINGS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.shipment_trackings", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaShipmentTrackings()));
        rowsShipmentTrackings.apply(Wait.on(writeResultShipmentTrackings.getFailedInserts()))
                .apply("COUNT MESSAGE", ParDo.of(new TemplatePipelineDataToBigQueryShopify.CountMessage("Shipment_trackings_pipeline_completed","shipment_trackings")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        pipeline.run();
    }


    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);
    }

    public static class TransformRowToInteger extends DoFn<TableRow, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(1);
        }
    }
    public static class CountMessage extends DoFn<TableRow, PubsubMessage>{
        private String messageDone;
        private String table;
        private String firstDistinctColon;
        private String secondDistinctColon;

        public CountMessage(String messageDone, String table, String firstDistinctColon, String secondDistinctColon) {
            this.messageDone = messageDone;
            this.table = table;
            this.firstDistinctColon = firstDistinctColon;
            this.secondDistinctColon = secondDistinctColon;

        }
        @ProcessElement
        public void processElement(ProcessContext c) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("table", table);
            attributes.put("first_distinct_colon", firstDistinctColon);
            attributes.put("second_distinct_colon", secondDistinctColon);
            PubsubMessage message = new PubsubMessage(messageDone.getBytes(), attributes);
            c.output(message);
        }
    }


}
