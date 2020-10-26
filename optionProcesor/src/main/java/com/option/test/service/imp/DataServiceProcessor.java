package com.option.test.service.imp;

import com.option.test.processor.DataProcessor;
import com.option.test.processor.imp.BigQueryProcess;
import com.option.test.service.DataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Created by Felipe González Alfaro on 23-10-20.
 */
@Component
public class DataServiceProcessor implements DataService {

    DataProcessor dataProcessor;

    private static final Logger logger = LoggerFactory.getLogger(DataServiceProcessor.class);

    @Autowired
    public DataServiceProcessor(@Qualifier("Dataproc") DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }


    @Override
    public void processData(String pathFile) {
        try {
            if (pathFile.contains("input/")) {
                logger.info("Start Process " + pathFile);
                this.dataProcessor.loadData(pathFile);
                this.dataProcessor.processData();
            }
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
        }
    }
}
