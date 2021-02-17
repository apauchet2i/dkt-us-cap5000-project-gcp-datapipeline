package org.datapipeline;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.beans.PropertyVetoException;
import java.sql.PreparedStatement;

import static org.datapipeline.models.OrderErrors.setParametersOrderErrorsSQL;
import static org.datapipeline.models.OrderItems.TransformJsonParDoOrderItemsShiphawk;
import static org.datapipeline.models.OrderItems.setParametersOrderItemsSQL;
import static org.datapipeline.models.OrderShipments.*;
import static org.datapipeline.models.OrderSources.TransformJsonParDoOrderSourcesShipHawk;
import static org.datapipeline.models.OrderSources.setParametersOrderSourcesSQL;
import static org.datapipeline.models.ShipmentTrackings.TransformJsonParDoShipmentTrackingsShipHawk;

public class TemplatePipelineDataToBigQueryShipHawkSQL {
    public static void main(String[] args) throws PropertyVetoException {

        String usernameSQL="cap5000";
        String passwordSQL="Mobilitech/20";
        String jdbcUrl="jdbc:mysql://51.91.122.200:3306/cap5000?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC";
        //String jdbcUrl = "jdbc:mysql://google/cap5000?cloudSqlInstance=dkt-us-data-lake-a1xq:us-west2:mulesoftdbinstance-staging&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=cap5000&password=" + passwordSQL + "&useUnicode=true&characterEncoding=UTF-8";

        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pCollectionDataJson = pipeline.apply("READ DATA IN JSON FILE", TextIO.read().from(options.getInputFile()));

        // ********************************************   ORDER SHIPMENTS TABLE   ********************************************
        PCollection<TableRow> rowsOrderShipments= pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SHIPMENTS", ParDo.of(new TransformJsonParDoOrderShipmentsShiphawk()));
        rowsOrderShipments.apply(JdbcIO.<TableRow>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "com.mysql.jdbc.Driver", jdbcUrl))
                .withStatement("insert into order_shipments (id,source,order_number,status,updated_at) values(?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE \n" +
                        " order_number= VALUES(order_number),\n" +
                        " status= VALUES(status),\n" +
                        " updated_at = VALUES(updated_at) ")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
                    @Override
                    public void setParameters(TableRow element, PreparedStatement preparedStatement) throws Exception {
                        setParametersOrderShipmentsSQL(element, preparedStatement);
                    }
                })
        );

        // ********************************************   ORDER SHIPMENTS ERROR    ********************************************
        PCollection<TableRow> rowsOrderShipmentsErrors = rowsOrderShipments.apply("TRANSFORM JSON TO TABLE ROW ERROR", ParDo.of(new mapOrderShipmentsErrorShipHawk()));
        rowsOrderShipmentsErrors.apply(JdbcIO.<TableRow>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "com.mysql.jdbc.Driver", jdbcUrl))
                .withStatement("insert into order_errors (order_number,error_type,updated_at,source) values(?,?,?,?) ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
                    @Override
                    public void setParameters(TableRow element, PreparedStatement preparedStatement) throws Exception {
                        setParametersOrderErrorsSQL(element, preparedStatement);
                    }
                })
        );

        // ********************************************   ORDER ITEMS TABLE   ********************************************
        PCollection<TableRow> rowsOrderItems = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER ITEMS", ParDo.of(new TransformJsonParDoOrderItemsShiphawk()));
        rowsOrderItems.apply(JdbcIO.<TableRow>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "com.mysql.jdbc.Driver", jdbcUrl))
                .withStatement("insert into order_items (id,shipment_id,source,name,price,quantity,updated_at) values(?,?,?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE \n" +
                        " name= VALUES(name),\n" +
                        " price= VALUES(price),\n" +
                        " quantity = VALUES(quantity),\n" +
                        " updated_at = VALUES(updated_at)" )
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {

                    @Override
                    public void setParameters(TableRow element, PreparedStatement preparedStatement) throws Exception {
                        setParametersOrderItemsSQL(element, preparedStatement);
                    }
                })
        );

        // ********************************************   ORDER SOURCES TABLE   ********************************************
        PCollection<TableRow> rowsOrderSources = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW ORDER SOURCES", ParDo.of(new TransformJsonParDoOrderSourcesShipHawk()));
        rowsOrderSources.apply(JdbcIO.<TableRow>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "com.mysql.jdbc.Driver", jdbcUrl))
                .withStatement("insert into order_sources (order_number,source,updated_at) values(?,?,?) " +
                        "ON DUPLICATE KEY UPDATE \n" +
                        " updated_at = VALUES(updated_at) ")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
                    @Override
                    public void setParameters(TableRow element, PreparedStatement preparedStatement) throws Exception {
                        setParametersOrderSourcesSQL(element, preparedStatement);
                    }
                })
        );

        // ********************************************   SHIPMENT TRACKINGS TABLE   ********************************************
        PCollection<TableRow> rowsShipmentTrackings = pCollectionDataJson.apply("TRANSFORM JSON TO TABLE ROW SHIPMENT TRACKINGS", ParDo.of(new TransformJsonParDoShipmentTrackingsShipHawk()));
        rowsShipmentTrackings.apply(JdbcIO.<TableRow>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "com.mysql.jdbc.Driver", jdbcUrl))
                .withStatement("insert into order_shipments (shipment_id,source,tracking_id,tracking_link,updated_at) values(?,?,?,?,?) ON DUPLICATE KEY UPDATE \n" +
                        " tracking_id = VALUES(tracking_id),\n" +
                        " tracking_link = VALUES(tracking_link),\n" +
                        " updated_at = VALUES(updated_at) " +
                        "ON DUPLICATE KEY UPDATE \n" +
                        " order_number= VALUES(order_number),\n" +
                        " status= VALUES(status),\n" +
                        " updated_at = VALUES(updated_at) " +
                        "ON DUPLICATE KEY UPDATE \n" +
                        " updated_at = VALUES(updated_at)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
                    @Override
                    public void setParameters(TableRow element, PreparedStatement preparedStatement) throws Exception {
                        setParametersOrderErrorsSQL(element, preparedStatement);
                    }
                })
        );

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
}
