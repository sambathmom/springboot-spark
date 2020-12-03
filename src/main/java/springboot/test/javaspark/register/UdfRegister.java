package springboot.test.javaspark.register;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import springboot.test.javaspark.interfaces.ReturnStringUdf;

public class UdfRegister {
    @Autowired
    private SparkSession sparkSession;

    @Bean
    public void registerUdf() {
        //===== Register udf
        sparkSession.udf()
                .register(
                        "returnName",
                        new ReturnStringUdf(),
                        DataTypes.StringType
                );
    }


}
