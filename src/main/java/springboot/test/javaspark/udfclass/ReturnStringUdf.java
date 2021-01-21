package springboot.test.javaspark.udfclass;

import org.apache.spark.sql.api.java.UDF0;
import springboot.test.javaspark.service.ReturnStringUdfService;

public class ReturnStringUdf implements UDF0 {
    @Override
    public Object call() throws Exception {
        return ReturnStringUdfService.returnString();
    }
}
