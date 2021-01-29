package org.polleyg;

import com.google.cloud.bigquery.*;

// Sample to run query script.
public class QueryScript {

    public static void main(String[] args) {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "MY_PROJECT_ID";
        String datasetName = "MY_DATASET_NAME";
        String tableName = "MY_TABLE_NAME";
        String query = //"SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY number,customer_id) row_number FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders`) WHERE row_number = 1";
        //"SELECT customer_id FROM ( SELECT ARRAY_AGG( t ORDER BY t.updated_at DESC LIMIT 1)[OFFSET(0)]  customer_id FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders` t GROUP BY number)";
        "DELETE FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders` d WHERE EXISTS (WITH redundant AS ( SELECT number,customer_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders` GROUP BY number, customer_id HAVING counter > 1) SELECT 1 FROM redundant WHERE d.customer_id = customer_id AND d.updated_at != updated_at )";

//        "WITH cte AS ("
//                + "SELECT number,customer_id,"
//                + "ROW_NUMBER() OVER (PARTITION BY number,updated_at"
//                + "(ORDER BY number,updated_at) row_num"
//
//                //+ "MAX(updated_at) AS updated_at,"
//                //+ "COUNT(*) AS counter"
//                + " FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders` ) "
//                //+ "GROUP BY number, customer_id"
//                //+ "HAVING counter > 1)"
//                //+ "DELETE FROM `dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders` d "
//                //+ "WHERE d.customer_id = customer_id AND d.updated_at != updated_at )";
//        + "DELETE FROM cte"
//                + "WHERE row_num >1";
        query(query);
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

