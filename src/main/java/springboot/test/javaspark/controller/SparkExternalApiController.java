package springboot.test.javaspark.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import springboot.test.javaspark.service.SparkExternalApiService;

import java.util.HashMap;
import java.util.Map;

@RestController
public class SparkExternalApiController {

    @Autowired
    private SparkExternalApiService sparkExternalApiService;

    @GetMapping("/data")
    public Map<String, Object> data() {
        Map<String, Object> data = new HashMap<>();
        data.put("message", "Successfully");
        data.put("data", "test");

        return data;
    }

    @GetMapping("/spark/data")
    public String getData() {
        return sparkExternalApiService.getData();
    }


    @GetMapping("/spark/data-frame")
    public String testSparkDF() throws ClassNotFoundException {
        return sparkExternalApiService.testSparkDF();
    }

    @GetMapping("/spark/data-frame/join")
    public String sparkDataFrameJoin(){
        return sparkExternalApiService.dataFrameJoin();
    }
}
