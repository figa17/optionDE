package com.option.test.config;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigService {

    @Bean
    public Bigquery getBigQueryProcess() {
        return (Bigquery) BigQueryOptions.getDefaultInstance().getService();
    }

}
