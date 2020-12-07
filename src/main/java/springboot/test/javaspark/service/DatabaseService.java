package springboot.test.javaspark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class DatabaseService {
    @Autowired
    private SparkSession sparkSession;

    public String readDataFromJdbc() {
        Properties connectionProperties = this.connectionProperties();
        Dataset<Row> jdbcDF = sparkSession.read().jdbc(connectionProperties.getProperty("url"), "cities", connectionProperties);

        jdbcDF.show();
        jdbcDF.printSchema();

        return "Read data from database";
    }

    public String writeDataToDatabase() {
        Properties connectionProperties = this.connectionProperties();
        Dataset<Row> appendCityDf = sparkSession.read()
                .option("multiLine", true)
                .json("src/main/resources/cities.json").toDF();

        appendCityDf.write()
                .mode(SaveMode.Ignore)
                .jdbc(connectionProperties.getProperty("url"), "cities", connectionProperties);

        Dataset<Row> jdbcDF = sparkSession.read().jdbc(connectionProperties.getProperty("url"), "cities", connectionProperties);
        jdbcDF.show();
        return "Spark write data to database";
    }


    private Properties connectionProperties() {
        Properties connectionProperties = new Properties();
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");
        connectionProperties.put("url", "jdbc:mysql://192.168.2.4:3306/test");
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "");

        return connectionProperties;
    }
}
