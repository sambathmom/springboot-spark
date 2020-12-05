package springboot.test.javaspark.hotelcontroller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import springboot.test.javaspark.datahotelservice.SparkHotelService;

@RestController
public class SparkHotelController {
    @Autowired
    private SparkHotelService sparkHotelService;

    @GetMapping("/spark/read/hotel")
    public String readHotelDataSpark() {
        return sparkHotelService.readHotelDataSpark();
    }

    @GetMapping("/spark/transform/hotel")
    public String transformHotelDataSpark() {
        return sparkHotelService.transformHotelDataSpark();
    }
}
