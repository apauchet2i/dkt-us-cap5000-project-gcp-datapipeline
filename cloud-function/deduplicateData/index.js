const google = require('googleapis');
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const {PubSub} = require('@google-cloud/pubsub');

exports.deduplicateData = function() {

      projectId = 'dkt-us-data-lake-a1xq';
      subscriptionName = 'projects/dkt-us-data-lake-a1xq/subscriptions/dkt-us-cap5000-project-end-datapipeline-sub';

      // Creates a client; cache this for further use
      const pubSubClient = new PubSub();
      const timeout = 60;

      let table;
      let firstAttribute;
      let secondAttribute;

      function listenForMessages(_callback) {
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

          console.log(JSON.parse(message.attributes["first_distinct_colon"]));
          console.log(JSON.parse(message.attributes["second_distinct_colon"]));

          table = JSON.parse(message.attributes["table"]);
          firstAttribute = JSON.parse(message.attributes["first_distinct_colon"]);
          secondAttribute = JSON.parse(message.attributes["second_distinct_colon"]);

          // "Ack" (acknowledge receipt of) the message
          message.ack();
        };

        // Listen for new messages until timeout is hit
        subscription.on('message', messageHandler);

        _callback();
      }

      async function query() {
          listenForMessages( async function () {
              console.log("query function");
              const sqlQuery = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders\` d 
                            WHERE EXISTS (WITH redundant AS (
                            SELECT @firstAttribute, @secondAttribute,
                            MAX(updated_at) AS updated_at,
                            COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.orders\`
                            GROUP BY @firstAttribute, @secondAttribute  
                            HAVING counter > 1)
                            SELECT * FROM redundant 
                            WHERE d.@firstAttribute = @firstAttribute AND d.@secondAttribute = @firstAttribute  AND d.updated_at != updated_at)`;

              // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
              const options = {
                  query: sqlQuery,
                  // Location must match that of the dataset(s) referenced in the query.
                  location: 'US',
                  params: {table:table, firstAttribute:firstAttribute, secondAttribute:secondAttribute},
              };
              // Run the query
              const [rows] = await bigquery.query(options);

              console.log('Rows:');
              rows.forEach(row => console.log(row));
          });
      }
      query()
};
