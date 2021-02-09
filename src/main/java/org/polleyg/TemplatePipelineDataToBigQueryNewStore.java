package org.polleyg;

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
import org.polleyg.models.OrderItems;
import org.polleyg.models.OrderShipments;
import org.polleyg.models.OrderSources;
import org.polleyg.models.OrderStatus;

import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.polleyg.models.OrderErrors.getTableSchemaOrderErrors;
import static org.polleyg.models.OrderItems.getTableSchemaOrderItems;
import static org.polleyg.models.OrderShipments.getTableSchemaOrderShipments;
import static org.polleyg.models.OrderSources.getTableSchemaOrderSources;
import static org.polleyg.models.OrderStatus.getTableSchemaOrderStatus;

public class TemplatePipelineDataToBigQueryNewStore {
    public static void main(String[] args) {

        String project = "dkt-us-data-lake-a1xq";
        String dataset = "dkt_us_test_cap5000";

        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pCollectionDataJson = pipeline.apply("READ DATA IN JSON FILE", TextIO.read().from(options.getInputFile()));
        //PCollection<String> pCollectionDataJson = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/upload/missing_customer_info.json"));
        //PCollection<String> pCollectionDataJson = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookNewStoreOrder-07_02_2021_19_14_55.json"));


        // ********************************************   ORDER STATUS TABLE   ********************************************
        PCollection<TableRow> rowsOrderStatus = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new OrderStatus.TransformJsonParDoOrderStatusNewStore()));
        WriteResult writeResultOrderStatus = rowsOrderStatus.apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_status", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderStatus()));

        rowsOrderStatus.apply(Wait.on(writeResultOrderStatus.getFailedInserts()))
                .apply("COUNT MESSAGE", ParDo.of(new CountMessage("Order_status_pipeline_completed","order_status","order_number","source")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   ORDER STATUS PAYMENT ERROR    ********************************************
        WriteResult writeResultOrderStatusError = rowsOrderStatus.apply("TRANSFORM DATA FOR ERROR", ParDo.of(new OrderStatus.mapOrderStatusErrorNewStore()))
                .apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_errors", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderErrors()));

        // ********************************************   ORDER SHIPMENTS TABLE   ********************************************
        PCollection<TableRow> rowsOrderShipments = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER STATUS", ParDo.of(new OrderShipments.TransformJsonParDoOrderShipmentsNewStore()));
        WriteResult writeResultOrderShipments = rowsOrderShipments.apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_shipments", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderShipments()));

        rowsOrderShipments.apply(Wait.on(writeResultOrderShipments.getFailedInserts()))
                .apply("COUNT MESSAGE", ParDo.of(new TemplatePipelineDataToBigQueryShipHawk.CountMessage("Order_shipments_pipeline_completed","order_status","order_number","source")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   ORDER SHIPMENTS ERROR    ********************************************
        WriteResult writeResultOrderShipmentsError = rowsOrderShipments.apply("TRANSFORM DATA FOR ERROR", ParDo.of(new OrderShipments.mapOrderShipmentsErrorNewStore()))
                .apply("WRITE DATA IN BIGQUERY ORDER STATUS TABLE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.order_errors", project,dataset))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderErrors()));

        // ********************************************   ORDER SOURCES TABLE   ********************************************
        PCollection<TableRow> rowsOrderSources = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SOURCES", ParDo.of(new OrderSources.TransformJsonParDoOrderSourcesNewStore()));
        WriteResult writeResultOrderSources = rowsOrderSources.apply("WRITE DATA IN BIGQUERY ORDER SOURCES TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_sources", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderSources()));
        rowsOrderSources.apply(Wait.on(writeResultOrderSources.getFailedInserts()))
                .apply("COUNT MESSAGE", ParDo.of(new TemplatePipelineDataToBigQueryShopify.CountMessage("Order_sources_pipeline_completed","order_sources","order_number","source")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));

        // ********************************************   ORDER ITEMS TABLE   ********************************************
        PCollection<TableRow> rowsOrderItems = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER ITEMS", ParDo.of(new OrderItems.TransformJsonParDoOrderItemsNewStore()));
        WriteResult writeResultOrderItems = rowsOrderItems.apply("WRITE DATA IN BIGQUERY ORDER ITEMS TABLE", BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.order_items", project,dataset))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(getTableSchemaOrderItems()));
        rowsOrderItems.apply(Wait.on(writeResultOrderItems.getFailedInserts()))
                .apply("COUNT MESSAGE", ParDo.of(new TemplatePipelineDataToBigQueryShopify.CountMessage("Order_items_pipeline_completed","order_items","shipment_id","source")))
                .apply("WRITE PUB MESSAGE", PubsubIO.writeMessages().to("projects/dkt-us-data-lake-a1xq/topics/dkt-us-cap5000-project-end-datapipeline"));


        pipeline.run();
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


}
