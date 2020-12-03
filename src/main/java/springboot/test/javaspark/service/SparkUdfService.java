package springboot.test.javaspark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import springboot.test.javaspark.interfaces.ReturnStringUdf;

@Service
public class SparkUdfService {

    @Autowired
    private SparkSession sparkSession;

    public String testUdf() {

        Dataset<Row> peopleDataset = sparkSession.read().option("multiLine", true)
                .json("src/main/resources/people.json");

        //=== register udf
        sparkSession.udf()
                .register(
                        "returnName",
                        new ReturnStringUdf(),
                        DataTypes.StringType
                );
        //===== Use udf
        peopleDataset = peopleDataset.withColumn("udfName", functions.callUDF("returnName"));
        peopleDataset.show();

        return "Practise udf: User define function.";
    }
}
