//gcloud --project=grey-sort-challenge functions deploy goWithTheDataFlow --stage-bucket gs://batch-pipeline --trigger-bucket gs://batch-pipeline
const google = require('googleapis');
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
