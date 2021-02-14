const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const {PubSub} = require('@google-cloud/pubsub');
const pubSubClient = new PubSub();
const maxInProgress = 1;
const timeout = 10;
const projectId = 'dkt-us-data-lake-a1xq';
const projectDataset = 'dkt_us_test_cap5000';

const subscriberOptions = {
    flowControl: {
        maxMessages: maxInProgress,
    },
};

exports.dktUsCap5000ProjectDeduplicateDataCustomers = function() {

    subscriptionName = 'projects/dkt-us-data-lake-a1xq/subscriptions/dkt-us-cap5000-project-datapipeline-customers-sub';
        console.log("start listen message function");
        // References an existing subscription
        const subscription = pubSubClient.subscription(
            subscriptionName,
            subscriberOptions
        );

        // Create an event handler to handle messages
        const messageHandler = message => {
            console.log(`One message`);
            console.log(`Received message ${message.id}:`);
            console.log(`\tData: ${message.data}`);
            message.ack();
        };
        subscription.on('message', messageHandler);
        setTimeout(() => {
            subscription.close();
            resolve("ok")
        }, timeout * 1000);

        console.log("begin function");
        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.customers\` d WHERE EXISTS (WITH redundant AS (SELECT id, lastname, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.customers\` GROUP BY id, lastname HAVING counter > 1) SELECT * FROM redundant WHERE d.id=id AND d.lastname=lastname AND d.updated_at != updated_at)`;

        const options = {
            query: sqlQuery,
            location: 'US',
        };

        bigquery.query(options);
};

exports.dktUsCap5000ProjectDeduplicateDataOrderItems = function() {

    async function query() {
        console.log("begin function");

        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_items\` d WHERE EXISTS (WITH redundant AS (SELECT id, shipment_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_items\` GROUP BY id, shipment_id HAVING counter > 1) SELECT * FROM redundant WHERE d.id=id AND d.shipment_id=shipment_id AND d.updated_at != updated_at)`;

        const options = {
            query: sqlQuery,
            location: 'US',
        };
        // Run the query

        const [rows] = await bigquery.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrderShipments = function() {

    async function query() {
        console.log("begin function");

        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_shipments\` d WHERE EXISTS (WITH redundant AS (SELECT id, status, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_shipments\` GROUP BY id, status HAVING counter > 1) SELECT * FROM redundant WHERE d.id=id AND d.status=status AND d.updated_at != updated_at)`;

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            location: 'US',
        };

        const [rows] = await bigquery.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrderSources = function() {

    async function query() {
        console.log("begin function");

        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_sources\` d WHERE EXISTS (WITH redundant AS (SELECT order_number, source, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_sources\` GROUP BY order_number, source HAVING counter > 1) SELECT * FROM redundant WHERE d.order_number=order_number AND d.source=source AND d.updated_at != updated_at)`;

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            location: 'US',
        };

        const [rows] = await bigquery.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrders = function() {

    async function query() {
        console.log("begin function");

        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders\` d WHERE EXISTS (WITH redundant AS (SELECT number, customer_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders\` GROUP BY number, customer_id HAVING counter > 1) SELECT * FROM redundant WHERE d.number=number AND d.customer_id=customer_id AND d.updated_at != updated_at)`;

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            location: 'US',
        };

        const [rows] = await bigquery.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrderStatus = function() {

    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();

    async function query() {
        console.log("begin function");

        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_status\` d WHERE EXISTS (WITH redundant AS (SELECT order_number, source, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_status\` GROUP BY order_number, source HAVING counter > 1) SELECT * FROM redundant WHERE d.order_number=order_number AND d.source=source AND d.updated_at != updated_at)`;

        const options = {
            query: sqlQuery,
            location: 'US',
        };

        const [rows] = await bigquery.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataShipmentTrackings = function() {

    async function query() {
        console.log("begin function");

        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.shipment_trackings\` d WHERE EXISTS (WITH redundant AS (SELECT shipment_id, tracking_id, MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.shipment_trackings\` GROUP BY shipment_id, tracking_id HAVING counter > 1) SELECT * FROM redundant WHERE d.shipment_id=shipment_id AND d.tracking_id=tracking_id AND d.updated_at != updated_at)`;

        const options = {
            query: sqlQuery,
            location: 'US',
        };
        // Run the query

        const [rows] = await bigquery.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};

exports.dktUsCap5000ProjectDeduplicateDataOrderErrors = function() {

    async function query() {
        console.log("begin function");

        const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_errors\` d WHERE EXISTS (WITH redundant AS (SELECT order_number, error_type, source MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.order_errors\` GROUP BY order_number, error_type, source HAVING counter > 1) SELECT * FROM redundant WHERE d.order_number=order_number AND d.error_type=error_type AND d.source=source AND d.updated_at != updated_at)`;

        const options = {
            query: sqlQuery,
            location: 'US',
        };
        // Run the query

        const [rows] = await bigquery.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    query();
};
