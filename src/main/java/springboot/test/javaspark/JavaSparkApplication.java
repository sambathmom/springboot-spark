package springboot.test.javaspark;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springboot.test.javaspark.datahotelservice.SparkHotelService;
import springboot.test.javaspark.service.DatabaseService;
import springboot.test.javaspark.service.FlightTransformService;

@SpringBootApplication
public class JavaSparkApplication implements ApplicationRunner {
    @Autowired
    private SparkHotelService hotelService;

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private FlightTransformService flightTransformService;

    public static void main(String[] args) {
        SpringApplication.run(JavaSparkApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        //hotelService.readHotelDataSpark();
        //databaseService.writeDataToDatabase();
        flightTransformService.flight();

        //databaseService.readDataFromJdbc();

    }
}
