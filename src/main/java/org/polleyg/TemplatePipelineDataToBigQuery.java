package org.polleyg;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

import org.polleyg.object.Customer.*;
import org.polleyg.object.Orders.*;
import org.polleyg.object.OrderItems.*;
import org.polleyg.object.OrderShipments.*;
import org.polleyg.object.OrderStatus.*;
import org.polleyg.object.ShipmentTrackings.*;
import org.polleyg.object.OrderSources.*;

import java.util.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.polleyg.object.Customer.getTableSchemaCustomer;
import static org.polleyg.object.OrderItems.getTableSchemaOrderItems;
import static org.polleyg.object.OrderShipments.getTableSchemaOrderShipments;
import static org.polleyg.object.OrderSources.getTableSchemaOrderSources;
import static org.polleyg.object.OrderStatus.getTableSchemaOrderStatus;
import static org.polleyg.object.Orders.*;
import static org.polleyg.object.ShipmentTrackings.getTableSchemaShipmentTrackings;

public class TemplatePipelineDataToBigQuery {
    public static void main(String[] args) {

        String project = "dkt-us-data-lake-a1xq";
        String dataset = "dkt_us_test_cap5000";

        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pCollectionDataJson = pipeline.apply("READ DATA IN JSON FILE", TextIO.read().from(options.getInputFile()));
        //PCollection<String> pcollection1 = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-25_11_2020_21_36_25.json"));
        //PCollection<String> pcollection1 = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-21_01_2021_21_17_48.json"));

        // ********************************************   ORDERS TABLE   ********************************************
        PCollection<TableRow> rowsOrders = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDERS", ParDo.of(new TransformJsonParDoOrders()));
        WriteResult writeResultOrders = rowsOrders.apply("WRITE DATA IN BIGQUERY ORDERS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.orders", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrder()));
        rowsOrders.apply(Wait.on(writeResultOrders.getFailedInserts()))
                // Transforms each row inserted to an Integer of value 1
                .apply("ONE PER INSERT ROW", ParDo.of(new TransformRowToInteger()))
                .apply("SUM INSERTED COUNTS", Sum.integersGlobally())
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Orders_pipeline_completed")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   CUSTOMERS TABLE   ********************************************
        PCollection<TableRow> rowsCustomers = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW CUSTOMERS", ParDo.of(new TransformJsonParDoCustomer()));
        WriteResult writeResultCustomers = rowsCustomers.apply("WRITE DATA IN BIGQUERY CUSTOMERS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.customers", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaCustomer()));
        rowsCustomers.apply(Wait.on(writeResultCustomers.getFailedInserts()))
                // Transforms each row inserted to an Integer of value 1
                .apply("ONE PER INSERT ROW", ParDo.of(new TransformRowToInteger()))
                .apply("SUM INSERTED COUNTS", Sum.integersGlobally())
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Customers_pipeline_completed")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   ORDER ITEMS TABLE   ********************************************
        PCollection<TableRow> rowsOrderItems = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER ITEMS", ParDo.of(new TransformJsonParDoOrderItems()));
        WriteResult writeResultOrderItems = rowsOrderItems.apply("WRITE DATA IN BIGQUERY ORDER ITEMS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_items", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderItems()));
        rowsOrderItems.apply(Wait.on(writeResultOrderItems.getFailedInserts()))
                // Transforms each row inserted to an Integer of value 1
                .apply("ONE PER INSERT ROW", ParDo.of(new TransformRowToInteger()))
                .apply("SUM INSERTED COUNTS", Sum.integersGlobally())
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Order_items_pipeline_completed")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   ORDER SOURCES TABLE   ********************************************
        PCollection<TableRow> rowsOrderSources = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SOURCES", ParDo.of(new TransformJsonParDoOrderSources()));
        WriteResult writeResultOrderSources = rowsOrderSources.apply("WRITE DATA IN BIGQUERY ORDER SOURCES TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_sources", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderSources()));
        rowsOrderSources.apply(Wait.on(writeResultOrderSources.getFailedInserts()))
                // Transforms each row inserted to an Integer of value 1
                .apply("ONE PER INSERT ROW", ParDo.of(new TransformRowToInteger()))
                .apply("SUM INSERTED COUNTS", Sum.integersGlobally())
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Order_sources_pipeline_completed")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   ORDER STATUS TABLE   ********************************************
        PCollection<TableRow> rowsOrderStatus = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new TransformJsonParDoOrderStatus()));
        WriteResult writeResultOrderStatus = rowsOrderStatus.apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_status", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderStatus()));
        rowsOrderStatus.apply(Wait.on(writeResultOrderStatus.getFailedInserts()))
                // Transforms each row inserted to an Integer of value 1
                .apply("ONE PER INSERT ROW", ParDo.of(new TransformRowToInteger()))
                .apply("SUM INSERTED COUNTS", Sum.integersGlobally())
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Order_status_pipeline_completed")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   ORDER SHIPMENTS TABLE   ********************************************
        PCollection<TableRow> rowsOrderShipments = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SHIPMENTS", ParDo.of(new TransformJsonParDoOrderShipments()));
        WriteResult writeResultOrderShipments = rowsOrderShipments.apply("WRITE DATA IN BIGQUERY ORDER SHIPMENTS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_items", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderShipments()));
        rowsOrderShipments.apply(Wait.on(writeResultOrderShipments.getFailedInserts()))
                // Transforms each row inserted to an Integer of value 1
                .apply("ONE PER INSERT ROW", ParDo.of(new TransformRowToInteger()))
                .apply("SUM INSERTED COUNTS", Sum.integersGlobally())
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Order_shipments_pipeline_completed")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   SHIPMENT TRACKINGS TABLE   ********************************************
        PCollection<TableRow> rowsShipmentTrackings = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW SHIPMENT TRACKINGS", ParDo.of(new TransformJsonParDoShipmentTrackings()));
        WriteResult writeResultShipmentTrackings = rowsShipmentTrackings.apply("WRITE DATA IN BIGQUERY SHIPMENT TRACKINGS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.shipment_trackings", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaShipmentTrackings()));
        rowsShipmentTrackings.apply(Wait.on(writeResultShipmentTrackings.getFailedInserts()))
                // Transforms each row inserted to an Integer of value 1
                .apply("ONE PER INSERT ROW", ParDo.of(new TransformRowToInteger()))
                .apply("SUM INSERTED COUNTS", Sum.integersGlobally())
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Shipment_trackings_pipeline_completed")))
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
    public static class CountMessage extends DoFn<Integer, PubsubMessage>{
        private String messageDone;

        public CountMessage(String messageDone) {
            this.messageDone = messageDone;

        }
        @ProcessElement
        public void processElement(ProcessContext c) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("rows_written", c.element().toString());
            PubsubMessage message = new PubsubMessage(messageDone.getBytes(), attributes);
            c.output(message);
        }
    }
}
