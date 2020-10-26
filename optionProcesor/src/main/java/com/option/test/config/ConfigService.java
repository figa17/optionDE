package com.option.test.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.dataproc.v1beta2.JobControllerClient;
import com.google.cloud.dataproc.v1beta2.JobControllerSettings;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

@Configuration
public class ConfigService {

    @Value("classpath:optionde-2a3ca1e38910.json")
    private Resource resource;

    @Value("${app.region}")
    private String region;

    @Bean
    public BigQuery getBigQueryProcess() throws IOException {

        GoogleCredentials credentials = GoogleCredentials.fromStream(resource.getInputStream())
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/bigquery"));
        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }


    @Bean
    public JobControllerClient getJobDataprocController() throws IOException {

        final String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

        final List<String> SCOPES = Arrays.asList(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.full_control",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/datastore",
                "https://www.googleapis.com/auth/pubsub",
                "https://www.googleapis.com/auth/compute");


        // Configure the settings for the cluster controller client.
        GoogleCredentials credentials = GoogleCredentials.fromStream(resource.getInputStream())
                .createScoped(SCOPES);

        CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);

        // Configure the settings for the job controller client.
        JobControllerSettings jobControllerSettings = JobControllerSettings.newBuilder()
                .setCredentialsProvider(credentialsProvider).setEndpoint(myEndpoint).build();

        return JobControllerClient.create(jobControllerSettings);
    }
}
