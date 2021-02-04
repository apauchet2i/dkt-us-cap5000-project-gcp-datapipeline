const google = require('googleapis');
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const {PubSub} = require('@google-cloud/pubsub');

exports.deduplicateData = function() {

  google.auth.getApplicationDefault(function (err, authClient) {
    if (err) {
      throw err;
    }
    // See https://cloud.google.com/compute/docs/authentication for more information on scopes
    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
      // Scopes can be specified either as an array or as a single, space-delimited string.
      authClient = authClient.createScoped([
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/compute'
      ]);
    }
    google.auth.getDefaultProjectId(function(err, projectId) {

      if (err || !projectId) {
        console.error(`Problems getting projectId (${projectId}). Err was: `, err);
        throw err;
      }
      projectId = 'dkt-us-data-lake-a1xq', // Your Google Cloud Platform project ID
      subscriptionName = 'projects/dkt-us-data-lake-a1xq/subscriptions/dkt-us-cap5000-project-end-datapipeline-sub' // Name for the new subscription to create

      // Creates a client; cache this for further use
      const pubSubClient = new PubSub();
      const timeout = 60;

      function listenForMessages() {
        // References an existing subscription
        const subscription = pubSubClient.subscription(subscriptionName);

        // Create an event handler to handle messages
        let messageCount = 0;
        const messageHandler = message => {
          console.log(`One message`);
          console.log(`Received message ${message.id}:`);
          console.log(`\tData: ${message.data}`);
          console.log(`\tAttributes: ${message.attributes}`);
          messageCount += 1;

          // "Ack" (acknowledge receipt of) the message
          message.ack();
        };

        // Listen for new messages until timeout is hit
        subscription.on('message', messageHandler);

      }

      listenForMessages();

      async function query() {
        var uniqueBigQueryCombinaison = {};
        uniqueBigQueryCombinaison["id"] = "latname"; // customers table
        uniqueBigQueryCombinaison["order_number"] = "error_type"; // order_errors table
        uniqueBigQueryCombinaison["shipment_id"] = "source"; // order_items table
        uniqueBigQueryCombinaison["id"] = "source"; // order_shipments table
        uniqueBigQueryCombinaison["order_number"] = "source"; // order_sources table
        uniqueBigQueryCombinaison["order_number"] = "source";// order_status table
        uniqueBigQueryCombinaison["number"] = "created_at";// orders table
        uniqueBigQueryCombinaison["shipment_id"] = "source"; // shipment_trackings table

        var bigQueryTableList = ["customers", "order_errors", "order_items", "order_shipments", "order_sources", "order_status", "orders", "shipment_trackings"];


        const query = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders\` d 
                            WHERE EXISTS (WITH redundant AS (
                            SELECT number, customer_id,
                            MAX(updated_at) AS updated_at,
                            COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders\`
                            GROUP BY number, customer_id 
                            HAVING counter > 1)
                            SELECT * FROM redundant 
                            WHERE d.customer_id = customer_id AND d.updated_at != updated_at)`;

        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
          query: query,
          // Location must match that of the dataset(s) referenced in the query.
          location: 'US',
          //params: {table: 'orders', firstAttribute: 'number', secondAttribute:'created_at'},
        };

        // Run the query as a job
        const [job] = await bigquery.createQueryJob(options);
        console.log(`Job ${job.id} started.`);

        // Wait for the query to finish
        const [rows] = await job.getQueryResults();

        // Print the results
        console.log('Rows:');
        rows.forEach(row => console.log(row));
      }

      query();

      });
    });
};
