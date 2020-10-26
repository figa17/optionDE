package com.option.test.processor;

/**
 * Created by Felipe González Alfaro on 23-10-20.
 */
public interface DataProcessor {

    void loadData(String inputData) throws IllegalAccessException;

    boolean processData();

    boolean saveResult();
}
