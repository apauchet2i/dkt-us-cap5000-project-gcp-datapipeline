package org.polleyg;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TemplatePipelineOrderErrors {
    public static String customer_id;
    public static String order_number;

    public static void main(String[] args) {
        String project = "dkt-us-data-lake-a1xq";
        String dataset = "dkt_us_test_cap5000";

        PipelineOptionsFactory.register(TemplatePipelineDataToBigQueryShopify.TemplateOptions.class);
        TemplatePipelineDataToBigQueryShopify.TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplatePipelineDataToBigQueryShopify.TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        //PCollection<String> pCollectionDataJson = pipeline.apply("READ DATA IN JSON FILE", TextIO.read().from(options.getInputFile()));
        PCollection<String> pCollectionDataJson = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-25_11_2020_21_36_25.json"));
        //PCollection<String> pCollectionDataJson = pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-21_01_2021_21_17_48.json"))

//        PCollection<String> orderNumber = rowsOrders.apply(ParDo.of(TemplatePipelineDataToBigQuery.Transform.splitColumn()));
//        PCollection<TableRow> pCollectionDataBigQueryStatus = pipeline.apply(
//                "Read from BigQuery query",
//                BigQueryIO.readTableRows()
//                        .fromQuery(String.format("SELECT s.order_number as order_number, s.source as source, s.type as type, s.status as status "
//                                + "FROM `%s.%s.order_status` s "
//                                + "WHERE s.order_number = %s UNION ALL "
//                                + "SELECT sh.id as order_number, sh.source as source, %s as type, sh.status as status "
//                                + "FROM `%s.%s.order_shipments` sh "
//                                + "WHERE sh.order_number = %s;", project, dataset, orderNumber,"shipment",project, dataset,orderNumber.get(0) ))
//                        .usingStandardSql());

        //PCollection<TableRow> customer_id = rowsOrders.apply(ParDo.of(new Transform("customer_id")));


        PCollection<TableRow> pCollectionDataPlateform = pipeline.apply(
                BigQueryIO.readTableRows()
                        .fromQuery(String.format("SELECT distinct order_number, source "
                                + "FROM %s.%s.order_sources "
                                + "GROUP BY order_number, source "
                                + "HAVING COUNT(*) > 1;", project, dataset))
                        .usingStandardSql());
        PCollection<String> jsondata = pCollectionDataPlateform.apply("JSon Transform", AsJsons.of(TableRow.class));

        jsondata.apply("TRANSFORM JSON TO TABLE ROW ORDER ITEMS", ParDo.of(new mapData()));


        //.apply("Convert Row",
        //        MapElements.into(TypeDescriptor.of(Customer.class)).via(Customer::fromTableRow));
        //pCollectionDataBigQueryStatus.apply(ParDo.of(new mapData(order_number)));
//        pCollectionDataBigQueryStatus.apply("WRITE DATA IN BIGQUERY ORDERS TABLE", BigQueryIO.writeTableRows()
//                .to(String.format("%s:%s.order_errors", project,dataset))
//                .withCreateDisposition(CREATE_IF_NEEDED)
//                .withWriteDisposition(WRITE_APPEND)
//                .withSchema(getTableSchemaOrder()));


//        PCollection<TableRow> pCollectionDataBigQueryStatus = pipeline.apply(
//                "Read from BigQuery query",
//                BigQueryIO.readTableRows()
//                        .fromQuery(String.format("SELECT * FROM `%s.%s.customers` WHERE id IS NULL OR lastname= '' OR lastname IS NULL OR firstname='' OR firstname IS NULL;",project, dataset ))
//                        .usingStandardSql());

        pipeline.run().waitUntilFinish();

    }
    public static class mapData extends DoFn<String, TableRow> {
        List<String> orderNumber = new ArrayList<>();
        Integer i = 1;

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            TableRow newTableRowErrorPlatform = new TableRow();
            System.out.println(c.element());
//            jsonArrayGlobal.add(c.element());
//            System.out.println(jsonArrayGlobal.size());
//            System.out.println(jsonArrayGlobal.toJSONString());
//            System.out.println(i);
            System.out.println(c.element().getClass());

            JSONArray jsonArray = new JSONArray();
            jsonArray.put(c.element());
            if(i == 3){
                orderNumber = IntStream.range(0, jsonArray.length())
                        .mapToObj(index -> ((JSONObject)jsonArray.get(index)).optString("order_number"))
                        .collect(Collectors.toList());
                System.out.println("orderNumber");
                System.out.println(orderNumber);
            }
            i++;

//            }
//            for (Object o : jsonArrayGlobal) {
//                JSONObject object = (JSONObject) o;
//                if (o.)
//            }
//            jsonArrayGlobal.add(c.element());
//
//            orderNumber.add(c.element().get("order_number").toString());
//
//            System.out.println(orderNumber);
//            System.out.println(c.element().size());
//
//            newTableRowErrorPlatform.set("order_number","orderNumber");
//            newTableRowErrorPlatform.set("error_type","Missing customer infos");
//            System.out.println("ok mapDATA");
//            System.out.println(c.element().toString());
//            if(i == (c.element().size())){
//                System.out.println("ok derniere case");
//            }
//            i++;
            c.output(newTableRowErrorPlatform);

        }
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }

    public static class Transform1 extends DoFn<TableRow, String>{
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println((c.element()));
            c.output("ok");
        }
    }

    public static class Transform extends DoFn<TableRow, TableRow> {
        private String colonName;
        private String customer_id;
        private String order_number;

        private Transform(String colonName) {
            this.colonName = colonName;
        }

        @ProcessElement
        public void processElement(ProcessContext c, @Element TableRow input) {

            if(input.get("customer_id").toString()!=null) {
                customer_id = input.get("customer_id").toString();
                order_number = input.get("number").toString();
                System.out.println(customer_id);
                System.out.println(order_number);
            }
        }
    }
}
