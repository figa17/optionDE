package com.option.test.config;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigService {

    @Bean
    public BigQuery getBigQueryProcess() {
        return BigQueryOptions.getDefaultInstance().getService();
    }

}
