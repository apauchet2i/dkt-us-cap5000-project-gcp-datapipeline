exports.dktUsCap5000ProjectDeduplicateDataCustomers = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';

    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();

    async function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.customers d WHERE EXISTS (WITH redundant AS (SELECT id, lastname, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.customers GROUP BY id, lastname HAVING counter > 1) SELECT * FROM redundant WHERE d.id=id AND d.lastname=lastname AND d.updated_at != updated_at)";

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            location: 'US',
        };
        // Run the query

        console.log(sqlQuery);
        bigquery.query(options).then(console.log("end function"));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrders = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';

    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();

    async function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders d WHERE EXISTS (WITH redundant AS (SELECT number, customer_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders GROUP BY number, customer_id HAVING counter > 1) SELECT * FROM redundant WHERE d.number=number AND d.customer_id=customer_id AND d.updated_at != updated_at)";

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            location: 'US',
        };
        // Run the query

        console.log(sqlQuery);
        bigquery.query(options).then(console.log("end function"));
    }

    query();
};