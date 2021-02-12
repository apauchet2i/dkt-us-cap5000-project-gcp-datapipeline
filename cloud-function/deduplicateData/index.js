const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

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

        const [rows] = await bigqueryClient.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrderItems = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';

    function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_items d WHERE EXISTS (WITH redundant AS (SELECT id, shipment_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_items GROUP BY id, shipment_id HAVING counter > 1) SELECT * FROM redundant WHERE d.id=id AND d.shipment_id=shipment_id AND d.updated_at != updated_at)";

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

exports.dktUsCap5000ProjectDeduplicateDataOrderShipments = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';

    function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_shipments d WHERE EXISTS (WITH redundant AS (SELECT id, status, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_shipments GROUP BY id, status HAVING counter > 1) SELECT * FROM redundant WHERE d.id=id AND d.status=status AND d.updated_at != updated_at)";

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

exports.dktUsCap5000ProjectDeduplicateDataOrderSources = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';

    function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_sources d WHERE EXISTS (WITH redundant AS (SELECT order_number, source, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_sources GROUP BY order_number, source HAVING counter > 1) SELECT * FROM redundant WHERE d.order_number=order_number AND d.source=source AND d.updated_at != updated_at)";

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

    async function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders d WHERE EXISTS (WITH redundant AS (SELECT number, customer_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders GROUP BY number, customer_id HAVING counter > 1) SELECT * FROM redundant WHERE d.number=number AND d.customer_id=customer_id AND d.updated_at != updated_at)";

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            location: 'US',
        };
        // Run the query

        const [rows] = await bigqueryClient.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrderStatus = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';

    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();

    async function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_status d WHERE EXISTS (WITH redundant AS (SELECT order_number, source, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders GROUP BY order_number, source HAVING counter > 1) SELECT * FROM redundant WHERE d.order_number=order_number AND d.source=source AND d.updated_at != updated_at)";

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            location: 'US',
        };
        // Run the query

        const [rows] = await bigqueryClient.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataShipmentTrackings = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';

    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();

    function query() {
        console.log("begin function");

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.shipment_trackings d WHERE EXISTS (WITH redundant AS (SELECT shipment_id, tracking_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000.shipment_trackings GROUP BY shipment_id, tracking_id HAVING counter > 1) SELECT * FROM redundant WHERE d.shipment_id=shipment_id AND d.tracking_id=tracking_id AND d.updated_at != updated_at)";

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
