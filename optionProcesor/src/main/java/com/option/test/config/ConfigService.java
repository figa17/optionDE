package com.option.test.config;

import com.google.api.services.bigquery.Bigquery;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

@Configuration
public class ConfigService {

    @Value("classpath:optionde-2a3ca1e38910.json")
    private Resource resource;

    @Bean
    public BigQuery getBigQueryProcess() throws IOException {

        GoogleCredentials credentials = GoogleCredentials. fromStream(resource.getInputStream())
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/bigquery"));
        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }

}
