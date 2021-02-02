//gcloud --project=grey-sort-challenge functions deploy goWithTheDataFlow --stage-bucket gs://batch-pipeline --trigger-bucket gs://batch-pipeline
const google = require('googleapis');
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

exports.goWithTheDataFlow = function(file, context) {

  console.log(`  Event: ${context.eventId}`);
  console.log(`  Event Type: ${context.eventType}`);
  console.log(`  Bucket: ${file.bucket}`);
  console.log(`  File: ${file.name}`);
  console.log(`  Metageneration: ${file.metageneration}`);
  console.log(`  Created: ${file.timeCreated}`);
  console.log(`  Updated: ${file.updated}`);

  const fileName = file.name;
  const eventType = context.eventType;

  console.log("File is: ", fileName);
  console.log("State is: ", eventType);

  if (eventType === 'google.storage.object.finalize' && fileName.indexOf('upload/') !== -1) {
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
        const dataflow = google.dataflow({ version: 'v1b3', auth: authClient });
        dataflow.projects.templates.create({
          projectId: projectId,
          resource: {
            parameters: {
              inputFile: `gs://${file.bucket}/${fileName}`
            },
            environment: {
              //serviceAccountEmail: 'tony.leon@decathlon.com',
              tempLocation: "gs://deploy-project-cap5000/temp",
              zone: "us-central1-f"
            },
            jobName: 'called-from-a-cloud-function-batch-pipeline-' + new Date().getTime(),
            gcsPath: 'gs://deploy-project-cap5000/template/pipeline'
          }
        }, function(err, response) {
          if (err) {
            console.error("Problem running dataflow template, error was: ", err);
          }
          else {
            // Import the Google Cloud client library using default credentials

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

              var bigQueryTableList = ["customers","order_errors","order_items","order_shipments","order_sources","order_status", "orders", "shipment_trackings"];


              const query = `DELETE FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.@table\` d 
                            WHERE EXISTS (WITH redundant AS (
                            SELECT @firstAttribute, @secondAttribute,
                            COUNT(*) AS counter FROM \`dkt-us-data-lake-a1xq.dkt_us_test_cap5000.bigQueryTableList.get(i)\`
                            GROUP BY @firstAttribute, @secondAttribute HAVING counter > 1)
                            SELECT 1 FROM redundant WHERE d.@firstAttribute = @firstAttribute AND d.@secondAttribute = @secondAttribute)`;

              // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
              const options = {
                query: query,
                // Location must match that of the dataset(s) referenced in the query.
                location: 'US',
                params: {table: 'orders', firstAttribute: 'number', secondAttribute:'created_at'},
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
          }
          console.log("Dataflow template response: ", response);
          //callback();
        });
      });
    });
  } else {
    console.log("Nothing to do here, ignoring.");
    //callback();
  }


};
