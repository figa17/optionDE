package com.option.test.processor.imp;

import com.google.cloud.bigquery.*;
import com.option.test.processor.DataProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Felipe González Alfaro on 23-10-20.
 */
@Component
public class BigQueryProcess implements DataProcessor {

    private BigQuery bigquery;
    private final static String DATASET = "optionde";
    private final static String TABLE_NAME = "inputData";

    private static final Logger logger = LoggerFactory.getLogger(BigQueryProcess.class);

    @Autowired
    public BigQueryProcess(BigQuery bigquery) {
        this.bigquery = bigquery;
    }

    @Override
    public void loadData(String inputData) {
        Schema schema = this.createsSchemaTable();
        this.createTable(schema, inputData);
    }


    @Override
    public boolean processData() {
        return false;
    }

    @Override
    public boolean saveResult() {
        return false;
    }


    private Schema createsSchemaTable() {

        Schema schema = Schema.of(
                Field.of("dt", StandardSQLTypeName.DATE),
                Field.of("AverageTemperature", StandardSQLTypeName.FLOAT64),
                Field.of("AverageTemperatureUncertainty", StandardSQLTypeName.FLOAT64),
                Field.of("Country", StandardSQLTypeName.STRING)
        );
//        createTable(datasetName, tableName, schema);
        logger.info("Schema: \n" + schema.toString());
        return schema;
    }

    private void createTable(Schema schema, String path) {
        try {
            // Skip header row in the file.
            CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();

            TableId tableId = TableId.of(DATASET, TABLE_NAME);
            ExternalTableDefinition externalTable = ExternalTableDefinition.newBuilder(path, csvOptions).setSchema(schema).build();
            TableInfo tableInfo = TableInfo.newBuilder(tableId, externalTable).build();

            bigquery.create(tableInfo);
            logger.info("Table: " + TABLE_NAME + " created successfully");
        } catch (BigQueryException e) {
            logger.error("Table was not created. Error: " + e.toString());
        }

    }

    private void deleteTable(){

    }

}
