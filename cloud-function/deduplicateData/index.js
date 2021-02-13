const google = require('googleapis');
const {PubSub} = require('@google-cloud/pubsub');

exports.deduplicateData = function() {

    projectId = 'dkt-us-data-lake-a1xq';
    projectDataset = 'dkt_us_test_cap5000';
    subscriptionName = 'projects/dkt-us-data-lake-a1xq/subscriptions/dkt-us-cap5000-project-end-datapipeline-sub';

    // Creates a client; cache this for further use
    const pubSubClient = new PubSub();

    let table;
    let firstAttribute;
    let secondAttribute;

    let listenForMessages = new Promise(function (resolve,reject){
        console.log("start listen message function");
        // References an existing subscription
        const subscription = pubSubClient.subscription(subscriptionName);

        // Create an event handler to handle messages
        let messageCount = 0;
        const messageHandler = message => {
            console.log(`One message`);
            console.log(`Received message ${message.id}:`);
            console.log(`\tData: ${message.data}`);
            console.log(`\tAttributes: ${JSON.stringify(message.attributes)}`);
            messageCount += 1;
            console.log("afficher attribut");
            console.log(JSON.stringify(message.attributes["first_distinct_colon"]));
            console.log(JSON.stringify(message.attributes["second_distinct_colon"]));
            console.log("end afficher attribut");

            table = (JSON.stringify(message.attributes["table"])).replace(/"/g, "");
            firstAttribute = (JSON.stringify(message.attributes["first_distinct_colon"])).replace(/"/g, "");
            secondAttribute = (JSON.stringify(message.attributes["second_distinct_colon"])).replace(/"/g, "");

            // "Ack" (acknowledge receipt of) the message
            message.ack();
            resolve("ok");
        };

        subscription.on('message', messageHandler);
    });

    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();

    async function query() {

        console.log("query function");
        console.log(table);
        console.log(firstAttribute);
        console.log(secondAttribute);
        console.log(typeof firstAttribute);

        const sqlQuery = "DELETE FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000." + table + " d WHERE EXISTS (WITH redundant AS (SELECT " + firstAttribute + ", " + secondAttribute + ", MAX(updated_at) AS updated_at, COUNT(*) AS counter FROM dkt-us-data-lake-a1xq.dkt_us_test_cap5000." + table + " GROUP BY " + firstAttribute + ", " + secondAttribute + " HAVING counter > 1) SELECT * FROM redundant WHERE d." + firstAttribute + "=" + firstAttribute + " AND d." + secondAttribute + "=" + secondAttribute + " AND d.updated_at != updated_at)";

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: sqlQuery,
            // Location must match that of the dataset(s) referenced in the query.
            location: 'US',
        };
        // Run the query

        console.log(sqlQuery);
        const [rows] = await bigquery.query(options);
        console.log(sqlQuery);
        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    listenForMessages.then(function(result) {
        query();
    });
};