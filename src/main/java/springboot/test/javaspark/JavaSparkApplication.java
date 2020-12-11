package springboot.test.javaspark;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springboot.test.javaspark.datahotelservice.SparkHotelService;

@SpringBootApplication
public class JavaSparkApplication implements ApplicationRunner {
    @Autowired
    private SparkHotelService hotelService;
    public static void main(String[] args) {
        SpringApplication.run(JavaSparkApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        hotelService.readHotelDataSpark();
    }
}
