package springboot.test.javaspark.service;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.util.function.Tuple2;
import scala.collection.Map;

import java.util.HashMap;
import java.util.Properties;

@Service
public class DatabaseService {
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    @Qualifier("connectionProperty")
    private Properties connectionProperties;

    public String readDataFromJdbc() {

        String code = "C001";
        String query = "select * from cities where code = " + "'" + code + "'";

        //==== Load all record from jdbc
        Dataset<Row> jdbcDF = sparkSession
                .read()
                .jdbc(connectionProperties.getProperty("url"), "cities", connectionProperties);

        //==== Use query to get data from jdbc
        Dataset<Row> readDF =
                sparkSession.read()
                        .format("jdbc")
                        .option("url", connectionProperties.getProperty("url"))
                        .option("driver", connectionProperties.getProperty("driver"))
                        .option("user", connectionProperties.getProperty("user"))
                        .option("password", connectionProperties.getProperty("password"))
                        .option("query", query)
                        .load();

        readDF.show();

        jdbcDF.show();
        jdbcDF.printSchema();

        return "Read data from database";
    }

    public String writeDataToDatabase() {
        Dataset<Row> appendCityDf = sparkSession.read()
                .option("multiLine", true)
                .json("src/main/resources/cities.json").toDF();

        // functions.row_number().over(Window.orderBy("name"))
        appendCityDf = sparkSession.read().jdbc(connectionProperties.getProperty("url"), "cities", connectionProperties);
        appendCityDf.show();
        appendCityDf = appendCityDf
            .withColumn("id_window", functions.row_number().over(Window.orderBy("name")))
            .withColumn("id",  functions.monotonically_increasing_id())
            .withColumn("code", appendCityDf.col("code"))
            .withColumn("name", appendCityDf.col("name"))
            .withColumn("population", appendCityDf.col("population"))
            .withColumn("created_at", functions.current_date())
            .withColumn("updated_at", functions.current_timestamp());

        appendCityDf.write()
                .mode(SaveMode.Append)
                .jdbc(connectionProperties.getProperty("url"), "cities", connectionProperties);

        Dataset<Row> jdbcDF = sparkSession
                .read()
                .jdbc(connectionProperties
                    .getProperty("url"), "cities", connectionProperties
                )
                .sqlContext()
                .sql("select * from cities where code = 'C001'");
        jdbcDF.show();


        //====== Write to province table
//        Dataset<Row> baseProvincesDF = sparkSession.read()
//                .option("multiLine", true)
//                .json("src/main/resources/provinces.json").toDF();
//
//        baseProvincesDF.show();

//        String row = jdbcDF.filter(jdbcDF.col("code").equalTo("C001")).select("id").collectAsList().get(0).toString();
//        System.out.println("=========== Row" + row);
//        Dataset<Row> provincesDF = baseProvincesDF
////                .withColumn("id", functions.row_number().over(Window.orderBy("name")));
//                .withColumn("country_id",
//                    functions.lit( jdbcDF
//                        .filter(jdbcDF
//                            .col("code")
//                            .equalTo(
//                                baseProvincesDF.col("country_code")
//                            )
//                        )
//                        .select("id")
//                        .collectAsList()
//                        .get(0).toString()));
//////
//
//            provincesDF = provincesDF.join(appendCityDf, appendCityDf.col("code").equalTo(provincesDF.col("country_code")));
//        provincesDF.show();

        return "Spark write data to database";
    }
}
