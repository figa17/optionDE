package com.option.test.controllers;

import io.swagger.annotations.Api;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Api(value = "Index")
public class IndexController {


    @GetMapping("/index")
    public ResponseEntity<String> index(){
        String response = "Data engineer test";
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

}
