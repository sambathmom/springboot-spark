package springboot.test.javaspark.datahotelservice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SparkHotelService {
    @Autowired
    private SparkSession sparkSession;

    public String readHotelDataSpark() {
        Dataset<Row> hotelDataDf = sparkSession.read()
        .json("src/main/resources/HOTELS/2020-12-04/ENG");

        hotelDataDf.createOrReplaceTempView("hotels");

        hotelDataDf.show(1);

        return "Read hotel data with spark";
    }

    public String transformHotelDataSpark() {
        Dataset<Row> hotels = sparkSession.sql("SELECT * FROM hotels LIMIT 1");
        hotels.show(1);

        //===== transform data
        Dataset<Row> hotelDF = hotels
                .withColumn("hotel", functions.explode(hotels.col("hotels")))
                .drop("auditData")
                .drop("error")
                .drop("from")
                .drop("hotels")
                .drop("to")
                .drop("total");
        hotelDF.show();
        hotelDF = hotelDF
                    .withColumn("categoryGroupCode", hotelDF.col("hotel.categoryGroupCode"))
                    .withColumn("code", hotelDF.col("hotel.code"))
                    .withColumn("name", hotelDF.col("hotel.name.content"));

        hotelDF.show(5);
        return "Transform hotel data spark";
    }
}
