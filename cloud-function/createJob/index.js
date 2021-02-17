const google = require('googleapis');

exports.dktUsCap5000ProjectDatapipelinejobSQL = function(file, context) {

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

  if (eventType === 'google.storage.object.finalize' ) {
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
      if (fileName.indexOf('shopify/') !== -1) {
        google.auth.getDefaultProjectId(function (err, projectId) {
          if (err || !projectId) {
            console.error(`Problems getting projectId (${projectId}). Err was: `, err);
            throw err;
          }
          const dataflow = google.dataflow({version: 'v1b3', auth: authClient});
          dataflow.projects.templates.create({
            projectId: projectId,
            resource: {
              parameters: {
                inputFile: `gs://${file.bucket}/${fileName}`
              },
              environment: {
                tempLocation: "gs://dkt-us-cap5000-project-deploy/temp/shopify",
                zone: "us-central1-f"
              },
              jobName: 'pipelineDataToBigQueryShopifySQL' + new Date().getTime(),
              gcsPath: 'gs://dkt-us-cap5000-project-deploy/template/TemplatePipelineDataToBigQueryShopifySQL'
            }
          }, function (err, response) {
            if (err) {
              console.error("Problem running dataflow template, error was: ", err);
            }
            console.log("Dataflow template response: ", response);
          });
        });
      }

      else if(fileName.indexOf('newstore/') !== -1){
        google.auth.getDefaultProjectId(function (err, projectId) {
          if (err || !projectId) {
            console.error(`Problems getting projectId (${projectId}). Err was: `, err);
            throw err;
          }
          const dataflow = google.dataflow({version: 'v1b3', auth: authClient});
          dataflow.projects.templates.create({
            projectId: projectId,
            resource: {
              parameters: {
                inputFile: `gs://${file.bucket}/${fileName}`
              },
              environment: {
                tempLocation: "gs://dkt-us-cap5000-project-deploy/temp/newstore",
                zone: "us-central1-f"
              },
              jobName: 'pipelineDataToBigQueryNewstoreSQL' + new Date().getTime(),
              gcsPath: 'gs://dkt-us-cap5000-project-deploy/template/pipelineDataToBigQueryNewStoreSQL'
            }
          }, function (err, response) {
            if (err) {
              console.error("Problem running dataflow template, error was: ", err);
            }
            console.log("Dataflow template response: ", response);
          });
        });
      }
      else if(fileName.indexOf('shiphawk/') !== -1){
        google.auth.getDefaultProjectId(function (err, projectId) {
          if (err || !projectId) {
            console.error(`Problems getting projectId (${projectId}). Err was: `, err);
            throw err;
          }
          const dataflow = google.dataflow({version: 'v1b3', auth: authClient});
          dataflow.projects.templates.create({
            projectId: projectId,
            resource: {
              parameters: {
                inputFile: `gs://${file.bucket}/${fileName}`
              },
              environment: {
                tempLocation: "gs://dkt-us-cap5000-project-deploy/temp/shiphawk",
                zone: "us-central1-f"
              },
              jobName: 'pipelineDataToBigQueryShiphawkSQL' + new Date().getTime(),
              gcsPath: 'gs://dkt-us-cap5000-project-deploy/template/pipelineDataToBigQueryShipHawkSQL'
            }
          }, function (err, response) {
            if (err) {
              console.error("Problem running dataflow template, error was: ", err);
            }
            console.log("Dataflow template response: ", response);
          });
        });
      }
      });

    if (fileName.indexOf('sap/') !== -1) {
      google.auth.getDefaultProjectId(function (err, projectId) {
        if (err || !projectId) {
          console.error(`Problems getting projectId (${projectId}). Err was: `, err);
          throw err;
        }
        const dataflow = google.dataflow({version: 'v1b3', auth: authClient});
        dataflow.projects.templates.create({
          projectId: projectId,
          resource: {
            parameters: {
              inputFile: `gs://${file.bucket}/${fileName}`
            },
            environment: {
              tempLocation: "gs://dkt-us-cap5000-project-deploy/temp/sap",
              zone: "us-central1-f"
            },
            jobName: 'pipelineDataToBigQuerySapSQL' + new Date().getTime(),
            gcsPath: 'gs://dkt-us-cap5000-project-deploy/template/pipelineDataToBigQuerySapSQL'
          }
        }, function (err, response) {
          if (err) {
            console.error("Problem running dataflow template, error was: ", err);
          }
          console.log("Dataflow template response: ", response);
        });
      });
    }
  } else {
    console.log("Nothing to do here, ignoring.");
  }
};
