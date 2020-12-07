package springboot.test.javaspark.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import springboot.test.javaspark.service.DatabaseService;

@RestController
public class DatabaseController {
    @Autowired
    private DatabaseService databaseService;

    @GetMapping(value = "/spark/read/db")
    public String readDataFromJdbc() {
        return databaseService.readDataFromJdbc();
    }

    @GetMapping(value = "/spark/write/db")
    public String writeDataToDatabase() {
        return databaseService.writeDataToDatabase();
    }
}
