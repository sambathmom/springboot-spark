package springboot.test.javaspark.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import springboot.test.javaspark.service.SparkUdfService;

@RestController()
public class SparkUdfController {
    @Autowired
    private SparkUdfService sparkUdfService;

    @GetMapping(value = "/spark/udf")
    public String testUdf() {
        return sparkUdfService.testUdf();
    }
}
