package org.polleyg;

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
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.Multimap;
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
public class TemplatePipelineStatusOrder {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("READ", TextIO.read().from(options.getInputFile()))
                //pipeline.apply("READ", TextIO.read().from("gs://dkt-us-ldp-baptiste-test/webhookShopify-05_01_2021_10_11_36.json"))

                .apply("TRANSFORM", ParDo.of(new TransformJsonParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:dkt_us_test_cap5000.orders", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchemaOrderStatus()));
        pipeline.run();
    }

    private static TableSchema getTableSchemaOrderStatus() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("order_number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("INTEGER").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("type").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }

    public static class TransformJsonParDo extends DoFn<String, List<TableRow>> {

        @ProcessElement
        public void mapJsonToBigqueryTable(ProcessContext c) throws Exception {
            List listTableRow = new ArrayList();
            JSONParser parser = new JSONParser();
            System.out.println(c.element().getClass());
            System.out.println(c.element());
            Object obj = parser.parse(c.element());
            JSONObject jsonObject = (JSONObject) obj;

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
            LocalDateTime now = LocalDateTime.now();
            String timeStampNow = dtf.format(now);

            Multimap<Object, Object> multimapStatusOrder = ArrayListMultimap.create();
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

            listTableRow.add(multimapStatusOrder);


            JSONObject jsonToBigQuery = new JSONObject(map);
            System.out.println(jsonToBigQuery);
            TableRow tableRow = convertJsonToTableRow(String.valueOf(jsonToBigQuery));
            System.out.println(tableRow);
            c.output(listTableRow);

        }
    }

    public static class WikiParDo extends DoFn<String, TableRow> {
        public static final String HEADER = "year,month,day,wikimedia_project,language,title,views";

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            if (c.element().equalsIgnoreCase(HEADER)) return;
            String[] split = c.element().split(",");
            if (split.length > 7) return;
            TableRow row = new TableRow();
            for (int i = 0; i < split.length; i++) {
                TableFieldSchema col = getTableSchemaOrderStatus().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }
}
