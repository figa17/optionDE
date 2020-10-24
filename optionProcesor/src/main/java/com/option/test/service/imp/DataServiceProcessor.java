package com.option.test.service.imp;

import com.option.test.processor.DataProcessor;
import com.option.test.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Felipe González Alfaro on 23-10-20.
 */
@Component
public class DataServiceProcessor implements DataService {

    DataProcessor dataProcessor;

    @Autowired
    public DataServiceProcessor(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }


    @Override
    public void processData(String pathFile) {
        this.dataProcessor.loadData("test");
    }
}
