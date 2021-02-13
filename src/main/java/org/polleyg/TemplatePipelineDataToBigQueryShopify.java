package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.joda.time.Duration;
import org.polleyg.models.Customer.*;
import org.polleyg.models.Orders.*;
import org.polleyg.models.OrderItems.*;
import org.polleyg.models.OrderShipments.*;
import org.polleyg.models.OrderStatus.*;
import org.polleyg.models.ShipmentTrackings.*;
import org.polleyg.models.OrderSources.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.polleyg.models.Customer.getTableSchemaCustomer;
import static org.polleyg.models.OrderErrors.getTableSchemaOrderErrors;
import static org.polleyg.models.OrderItems.getTableSchemaOrderItems;
import static org.polleyg.models.OrderShipments.getTableSchemaOrderShipments;
import static org.polleyg.models.OrderSources.getTableSchemaOrderSources;
import static org.polleyg.models.OrderStatus.getTableSchemaOrderStatus;
import static org.polleyg.models.Orders.*;
import static org.polleyg.models.ShipmentTrackings.getTableSchemaShipmentTrackings;

public class TemplatePipelineDataToBigQueryShopify {
    public static void main(String[] args) {

        String project = "dkt-us-data-lake-a1xq";
        String dataset = "dkt_us_test_cap5000";

        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options2 = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline2 = Pipeline.create(options2);

        PCollection<String> pCollectionDataJson = pipeline.apply("READ DATA IN JSON FILE", TextIO.read().from(options.getInputFile()));
        //PCollection<String> pCollectionDataJson = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/upload/missing_customer_info.json"));
        //PCollection<String> pCollectionDataJson = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-21_01_2021_21_17_48.json"));

        final PCollection<String> pCollectionDataJson2 = pipeline.apply("READ DATA IN JSON FILE", TextIO.read().from(options.getInputFile()));
        PCollection<TableRow> rowsOrders = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDERS", ParDo.of(new TransformJsonParDoOrders()));
        final PCollection<Void> afterSingleton = rowsOrders
                .apply("singleton#task", ParDo.of(new DoFn<TableRow, Void>() {
                    @ProcessElement  // (3)
                    public void onElement(final OutputReceiver<Void> output) {
                        WriteResult writeResultOrders = rowsOrders.apply("WRITE DATA IN BIGQUERY ORDERS TABLE", BigQueryIO.writeTableRows()
                                .to(String.format("%s:%s.orders", project,dataset))
                                .withCreateDisposition(CREATE_IF_NEEDED)
                                .withWriteDisposition(WRITE_APPEND)
                                .withSchema(getTableSchemaOrder()));
                        output.output(null);
                    }
                }));
        pCollectionDataJson2
                .apply("output#synchro", Wait.on(afterSingleton)) // (4)
                .apply("FORMAT MESSAGE ORDERS", ParDo.of(new CountMessageTest("Orders_pipeline_completed","orders","number","customer_id")))
                .apply("WRITE PUB MESSAGE CUSTOMERS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));


        // ********************************************   ORDERS TABLE   ********************************************
//        PCollection<TableRow> rowsOrders = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDERS", ParDo.of(new TransformJsonParDoOrders()));
//        WriteResult writeResultOrders = rowsOrders.apply("WRITE DATA IN BIGQUERY ORDERS TABLE", BigQueryIO.writeTableRows()
//                        .to(String.format("%s:%s.orders", project,dataset))
//                        .withCreateDisposition(CREATE_IF_NEEDED)
//                        .withWriteDisposition(WRITE_APPEND)
//                        .withSchema(getTableSchemaOrder()));
//        rowsOrders.apply(Window.<TableRow>into(FixedWindows.of(Duration.standardSeconds(50))))
//        .apply("FORMAT MESSAGE ORDERS", ParDo.of(new CountMessage("Orders_pipeline_completed","orders","number","customer_id")))
//        .apply("WRITE PUB MESSAGE CUSTOMERS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   CUSTOMERS TABLE   ********************************************
        PCollection<TableRow> rowsCustomers = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW CUSTOMERS", ParDo.of(new TransformJsonParDoCustomer()));
        rowsCustomers.apply("WRITE DATA IN BIGQUERY CUSTOMERS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.customers", project,dataset))
                .optimizedWrites()
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaCustomer()));
        rowsCustomers.apply(Window.<TableRow>into(FixedWindows.of(Duration.standardSeconds(50))))
                .apply("FORMAT MESSAGE CUSTOMERS", ParDo.of(new CountMessage("Customers_pipeline_completed","customers","id","lastname")))
                .apply("WRITE PUB MESSAGE CUSTOMERS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-customers"));

        // ********************************************   CUSTOMERS ERRORS   ********************************************
        PCollection<TableRow> rowsCustomersError = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW CUSTOMERS", ParDo.of(new mapOrderCustomersError()));
        rowsCustomersError.apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_errors", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderErrors()));

        // ********************************************   ORDER ITEMS TABLE   ********************************************
        PCollection<List<TableRow>> rowsOrderItemsList = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new TransformJsonParDoOrderItemsShopifyList()));
        PCollection<TableRow> rowsOrderItems = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER ITEMS", ParDo.of(new TransformJsonParDoOrderItemsShopify()));
        rowsOrderItems.apply("WRITE DATA IN BIGQUERY ORDER ITEMS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_items", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderItems()));
        rowsOrderItemsList.apply(Window.<List<TableRow>>into(FixedWindows.of(Duration.standardSeconds(50))))
                .apply("FORMAT MESSAGE ORDER ITEMS", ParDo.of(new CountMessageList("Order_items_pipeline_completed","order_items","shipment_id","source")))
                .apply("WRITE PUB MESSAGE ORDER ITEMS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-order-items"));

        // ********************************************   ORDER SOURCES TABLE   ********************************************
        PCollection<TableRow> rowsOrderSources = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SOURCES", ParDo.of(new TransformJsonParDoOrderSourcesShopify()));
        WriteResult writeResultOrderSources = rowsOrderSources.apply("WRITE DATA IN BIGQUERY ORDER SOURCES TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_sources", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderSources()));
        rowsOrderSources.apply(Window.<TableRow>into(FixedWindows.of(Duration.standardSeconds(50))))
                .apply("FORMAT MESSAGE ORDER SOURCES", ParDo.of(new CountMessage("Order_sources_pipeline_completed","order_sources","order_number","source")))
                .apply("WRITE PUB MESSAGE ORDER SOURCES", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-order-sources"));

        // ********************************************   ORDER STATUS TABLE   ********************************************
        PCollection<List<TableRow>> rowOrderStatusList = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new TransformJsonParDoOrderStatusShopifyList()));
        PCollection<TableRow> rowsOrderStatus = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new TransformJsonParDoOrderStatusShopify()));
        WriteResult writeResultOrderStatus = rowsOrderStatus.apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_status", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderStatus()));

        rowOrderStatusList.apply(Window.<List<TableRow>>into(FixedWindows.of(Duration.standardSeconds(50))))
                .apply("FORMAT MESSAGE ORDER STATUS", ParDo.of(new CountMessageList("Order_status_pipeline_completed","order_status","order_number","source")))
                .apply("WRITE PUB MESSAGE ORDER STATUS", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-order-status"));

        // ********************************************   ORDER STATUS PAYMENT ERROR    ********************************************
            WriteResult writeResultOrderStatusError = rowsOrderStatus.apply("TRANSFORM DATA FOR ERROR", ParDo.of(new mapOrderStatusError()))
                .apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_errors", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderErrors()));

        // ********************************************   ORDER SHIPMENTS TABLE   ********************************************
        PCollection<List<TableRow>> rowsOrderShipmentsList = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new TransformJsonParDoOrderShipmentsShopifyList()));
        PCollection<TableRow> rowsOrderShipments = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SHIPMENTS", ParDo.of(new TransformJsonParDoOrderShipmentsShopify()));
        WriteResult writeResultOrderShipments = rowsOrderShipments.apply("WRITE DATA IN BIGQUERY ORDER SHIPMENTS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_shipments", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderShipments()));
        rowsOrderShipmentsList.apply(Window.<List<TableRow>>into(FixedWindows.of(Duration.standardSeconds(50))))
                .apply("FORMAT MESSAGE ORDE", ParDo.of(new CountMessageList("Order_shipments_pipeline_completed","order_shipments","id","source")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-order-shipments"));

        // ********************************************   ORDER SHIPMENTS ERROR    ********************************************
        WriteResult writeResultOrderShipmentsError = rowsOrderShipments.apply("TRANSFORM DATA FOR ERROR", ParDo.of(new mapOrderShipmentsError()))
                .apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_errors", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderErrors()));

        // ********************************************   SHIPMENT TRACKINGS TABLE   ********************************************
        PCollection<List<TableRow>> rowsShipmentTrackingsList = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new TransformJsonParDoShipmentTrackingsShopifyList()));
        PCollection<TableRow> rowsShipmentTrackings = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW SHIPMENT TRACKINGS", ParDo.of(new TransformJsonParDoShipmentTrackingsShopify()));
        WriteResult writeResultShipmentTrackings = rowsShipmentTrackings.apply("WRITE DATA IN BIGQUERY SHIPMENT TRACKINGS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.shipment_trackings", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaShipmentTrackings()));
        rowsShipmentTrackingsList.apply(Window.<List<TableRow>>into(FixedWindows.of(Duration.standardSeconds(50))))
                .apply("COUNT MESSAGE", ParDo.of(new CountMessageList("Shipment_trackings_pipeline_completed","shipment_trackings","shipment_id","source")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-datapipeline-shipment-trackings"));

        // ********************************************   SHIPMENT TRACKINGS ERROR   ********************************************
        PCollection<TableRow> rowsShipmentTrackingsError = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW CUSTOMERS", ParDo.of(new mapShipmentTrackingErrorShopify()));
        WriteResult writeResultShipmentTrackingsError = rowsShipmentTrackingsError
                .apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_errors", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderErrors()));

        pipeline.run().waitUntilFinish();


    }



    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);
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

    public static class CountMessageTest extends DoFn<String, PubsubMessage>{
        private String messageDone;
        private String table;
        private String firstDistinctColon;
        private String secondDistinctColon;

        public CountMessageTest(String messageDone, String table, String firstDistinctColon, String secondDistinctColon) {
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

    public static class CountMessageList extends DoFn<List<TableRow>, PubsubMessage>{
        private String messageDone;
        private String table;
        private String firstDistinctColon;
        private String secondDistinctColon;

        public CountMessageList(String messageDone, String table, String firstDistinctColon, String secondDistinctColon) {
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

//comment
}
