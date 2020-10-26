package com.option.test.processor.imp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.option.test.processor.DataProcessor;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Felipe González Alfaro on 25-10-20.
 */
public class DataprocProcessor implements DataProcessor {
    @Override
    public void loadData(String inputData) {

    }

    @Override
    public boolean processData() {
        return false;
    }

    @Override
    public boolean saveResult() {
        return false;
    }


    String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

    final List<String> SCOPES = Arrays.asList(
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/datastore",
            "https://www.googleapis.com/auth/pubsub",
            "https://www.googleapis.com/auth/compute");


    // Configure the settings for the cluster controller client.
    InputStream in = Dataproc.class.getClassLoader().getResourceAsStream("optionde.json");
    GoogleCredentials credentials = GoogleCredentials.fromStream(in)
            .createScoped(SCOPES);

    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
//
//        ClusterControllerSettings clusterControllerSettings = ClusterControllerSettings.newBuilder()
//                .setCredentialsProvider(credentialsProvider).setEndpoint(myEndpoint).build();

    // Configure the settings for the job controller client.
    JobControllerSettings jobControllerSettings = JobControllerSettings.newBuilder()
            .setCredentialsProvider(credentialsProvider).setEndpoint(myEndpoint).build();



     try {
//            ClusterControllerClient clusterControllerClient = ClusterControllerClient.create(clusterControllerSettings);

        JobControllerClient jobControllerClient = JobControllerClient.create(jobControllerSettings);

        // Configure the settings for our cluster.
//            InstanceGroupConfig masterConfig = InstanceGroupConfig.newBuilder()
//                    .setMachineTypeUri("n1-standard-1")
//                    .setNumInstances(1)
//                    .build();
//            InstanceGroupConfig workerConfig = InstanceGroupConfig.newBuilder()
//                    .setMachineTypeUri("n1-standard-1")
//                    .setNumInstances(2)
//                    .build();
//            ClusterConfig clusterConfig = ClusterConfig.newBuilder()
//                    .setMasterConfig(masterConfig)
//                    .setWorkerConfig(workerConfig)
//                    .build();
//            // Create the cluster object with the desired cluster config.
//            Cluster cluster = Cluster.newBuilder().setClusterName(clusterName)
//                    .setConfig(clusterConfig)
//                    .build();
//            // Create the Cloud Dataproc cluster.
//            OperationFuture<Cluster, ClusterOperationMetadata> createClusterAsyncRequest =
//                    clusterControllerClient.createClusterAsync(projectId, region, cluster);
//            Cluster response = createClusterAsyncRequest.get();

        // Configure the settings for our job.

        JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();

        String jobFilePath = "gs://optio-de/input/process.py";
        PySparkJob pySparkJob = PySparkJob.newBuilder()
                .setMainPythonFileUri(jobFilePath)
                .addArgs("gs://optio-de/input/GlobalLandTemperaturesByCity.csv")
//                    .setArgs(0,"gs://optio-de/input/GlobalLandTemperaturesByCountry.csv")
                .build();

        Job job = Job.newBuilder().setPlacement(jobPlacement).setPysparkJob(pySparkJob).build();

        // Submit an asynchronous request to execute the job.
        OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest = jobControllerClient.submitJobAsOperationAsync(projectId, region, job);

        submitJobAsOperationAsyncRequest.get();


        // Print out a success message.
//            System.out.printf("Cluster created successfully: %s", response.getClusterName());
//            clusterControllerClient.close();
        // Delete the cluster.
//            OperationFuture<Empty, ClusterOperationMetadata> deleteClusterAsyncRequest =
//                    clusterControllerClient.deleteClusterAsync(projectId, region, clusterName);
//            deleteClusterAsyncRequest.get();

        System.out.println(String.format("Cluster \"%s\" successfully deleted.", clusterName));


    } catch (Exception e) {
        System.err.println(String.format("Error executing createCluster: %s ", e.getMessage()));

    }
}
