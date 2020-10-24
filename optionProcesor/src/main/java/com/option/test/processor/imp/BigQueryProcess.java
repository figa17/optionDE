package com.option.test.processor.imp;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.*;
import com.option.test.processor.DataProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Felipe González Alfaro on 23-10-20.
 */
@Component
public class BigQueryProcess implements DataProcessor {

    Bigquery bigquery;

    @Autowired
    public BigQueryProcess(Bigquery bigquery) {
        this.bigquery = bigquery;
    }

    @Override
    public void loadData(String inputData) {
        Schema schema = this.createsSchemaTable();

    }

    @Override
    public boolean processData() {
        return false;
    }


    private Schema createsSchemaTable() {
        String datasetName = "MY_DATASET_NAME";
        String tableName = "MY_TABLE_NAME";
        Schema schema = Schema.of(
                Field.of("dt", StandardSQLTypeName.DATE),
                Field.of("AverageTemperature", StandardSQLTypeName.FLOAT64),
                Field.of("AverageTemperatureUncertainty", StandardSQLTypeName.FLOAT64),
                Field.of("Country", StandardSQLTypeName.STRING)
        );
//        createTable(datasetName, tableName, schema);

        return schema;
    }


}
