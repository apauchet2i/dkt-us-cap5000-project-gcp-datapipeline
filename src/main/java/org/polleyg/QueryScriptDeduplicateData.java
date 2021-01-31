package org.polleyg;

import com.google.cloud.bigquery.*;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

// Sample to run query script.
public class QueryScriptDeduplicateData {

    public static void main(String[] args) {
        // args[0] = tableName
        List<String> bigQueryTableList = Arrays.asList("customers","order_errors","order_items","order_shipments","order_sources","order_status", "orders", "shipment_trackings");
        List<Pair<String, String>> uniqueBigQueryCombinaison = new ArrayList<Pair<String, String>>();
        uniqueBigQueryCombinaison.add(Pair.of("id", "lastname")); // customers table
        uniqueBigQueryCombinaison.add(Pair.of("order_number", "error_type")); // order_errors table
        uniqueBigQueryCombinaison.add(Pair.of("shipment_id", "source")); // order_items table
        uniqueBigQueryCombinaison.add(Pair.of("id", "source")); // order_shipments table
        uniqueBigQueryCombinaison.add(Pair.of("order_number", "source")); // order_sources table
        uniqueBigQueryCombinaison.add(Pair.of("order_number", "source")); // order_status table
        uniqueBigQueryCombinaison.add(Pair.of("number", "created_at")); // orders table
        uniqueBigQueryCombinaison.add(Pair.of("shipment_id", "source")); // shipment_trackings table

        String projectId = "MY_PROJECT_ID";
        String datasetName = "MY_DATASET_NAME";

        for (int i =0; i<bigQueryTableList.size();i++) {
                String query = "DELETE FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000." + bigQueryTableList.get(i) + "` d "
                        + "WHERE EXISTS (WITH redundant AS ( "
                        + "SELECT " + uniqueBigQueryCombinaison.get(i).getLeft() + "," + uniqueBigQueryCombinaison.get(i).getRight()  + ", "
                        + "COUNT(*) AS counter FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000." + bigQueryTableList.get(i) + "` "
                        + "GROUP BY " + uniqueBigQueryCombinaison.get(i).getLeft()  + "," + uniqueBigQueryCombinaison.get(i).getRight()  + " HAVING counter > 1) "
                        + "SELECT 1 FROM redundant WHERE d." + uniqueBigQueryCombinaison.get(i).getLeft()  + "=" + uniqueBigQueryCombinaison.get(i).getLeft()  + " AND d." + uniqueBigQueryCombinaison.get(i).getRight()  + "=" + uniqueBigQueryCombinaison.get(i).getRight()  + ")";

                query(query);
            }
        }

    public static void query(String query) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult results = bigquery.query(queryConfig);

            results
                    .iterateAll()
                    .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

            System.out.println("Query performed successfully.");
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query not performed \n" + e.toString());
        }
    }
}
// [END bigquery_query]

