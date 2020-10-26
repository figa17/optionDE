package com.option.test.processor.imp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1beta2.*;
import com.option.test.processor.DataProcessor;
import com.option.test.service.imp.DataServiceProcessor;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by Felipe González Alfaro on 25-10-20.
 */
@Component("Dataproc")
public class DataprocProcessor implements DataProcessor {

    @Value("${app.clusterName}")
    private String clusterName;

    @Value("${app.region}")
    private String region;

    @Value("${app.projectId}")
    private String projectId;

    @Value("${app.filePysparkPath}")
    private String jobFilePath;

    private String pathData;

    private JobControllerClient jobControllerClient;

    private static final Logger logger = LoggerFactory.getLogger(DataprocProcessor.class);

    @Autowired
    public DataprocProcessor(JobControllerClient jobControllerClient) throws ExecutionException, InterruptedException {
        this.jobControllerClient = jobControllerClient;
    }

    @Override
    public void loadData(String inputData) {

        if (inputData == null) {
            throw new IllegalArgumentException("Path data cant be null");
        }

        this.pathData = inputData;
    }

    @Override
    public boolean processData() {

        try {
            // Configure the settings for our job.
            JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();

            logger.info("creating pyspark job");
            PySparkJob pySparkJob = PySparkJob.newBuilder()
                    .setMainPythonFileUri(jobFilePath)
                    .addArgs(this.pathData)
                    .build();

            Job job = Job.newBuilder().setPlacement(jobPlacement).setPysparkJob(pySparkJob).build();


            logger.info("Submit job.");
            // Submit an asynchronous request to execute the job.
            OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest = jobControllerClient.submitJobAsOperationAsync(projectId, region, job);

            submitJobAsOperationAsyncRequest.get();
            return true;
        } catch (Exception e) {
            logger.error(String.format("Error executing createCluster: %s ", e.getMessage()));
            return false;
        }

    }

    @Override
    public boolean saveResult() {
        return false;
    }

}
