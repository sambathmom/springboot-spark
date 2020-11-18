package springboot.test.javaspark.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.json.JSONObject;
import org.json4s.jackson.Serialization;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.*;

@Service
public class SparkExternalApiService {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private SparkConf sparkConf;

    @Autowired
    private JavaSparkContext javaSparkContext;
    public String getData() {

        //======== Start initail JSON Data for test
        ArrayList<Object> baseData = new ArrayList<Object>();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("message", "Successfully");
        jsonObject.put("status", 200);

        JSONObject data = new JSONObject();
        data.put("name", "Name1");
        data.put("age", 11);

        JSONObject data1 = new JSONObject();
        data1.put("name", "Name1");
        data1.put("age", 30);

        ArrayList<Object> arrayList = new ArrayList<Object>();

        arrayList.add(data);
        arrayList.add(data1);
        arrayList.add(data);
        arrayList.add(data1);

        jsonObject.put("data", arrayList);
        baseData.add(jsonObject);
        //========= End initial JSON data


        //======== Read json data to spark dataFrame
        List<String> dataTestData = Arrays.asList(baseData.toString());
        var dataRdd = javaSparkContext.parallelize(dataTestData);
        Dataset<Row> dataset = sparkSession.read().option("mutilpleLine", true).json(dataRdd);
        dataset.show();

        Dataset<Row> datasetData = dataset.select(dataset.col("data"));
        datasetData.show();

        //======= Explode json array to dataFrame
        var dfData = dataset.select(explode(dataset.col("data"))).toDF("data");
        dfData.show();

        var dfDataContent = dfData.select("data.age", "data.name");
        dfDataContent.show();

        //======== GroupBy
        dfDataContent.groupBy("age").count().show();

        //======= Filter dataFrame
        dfDataContent = dfDataContent.filter(dfDataContent.col("age").gt(20));
        dfDataContent.show();

        //======= Convert dataFrame to json
        var age = dfDataContent.toJSON().collectAsList().toString();
        System.out.println("=========== Age: " + age);


        dfDataContent.createOrReplaceTempView("data_content");
       // var dfMaxAge = dfDataContent.select("select * from data_content where age = " + max(dfDataContent.col("age"))).toDF();
      //  dfMaxAge.show();
        var dfDataContentMaxAge = dfDataContent.select(max(dfDataContent.col("age")).alias("age"));
        dfDataContentMaxAge.show();


        //========== Test JavaRDD
        JavaRDD<String> lines = javaSparkContext.textFile("src/main/resources/wordcount.txt");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        System.out.println("======== lineLengths " + lineLengths);
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println("============ totalLength " + totalLength);

//        var dfDataContentMaxAgeRow = dfDataContent.withColumn("age", dfDataContent.col("age"))
//                                     .withColumn("name", dfDataContent.col("name"))
//                                     .where(max(dfDataContent.col("age")))
//                                     .toDF();
//
//        dfDataContentMaxAgeRow.show();


        //========= Accumulator
        LongAccumulator accum = javaSparkContext.sc().longAccumulator();
        javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> {
            accum.add(x);
        });
        accum.value();
        // returns 10
        System.out.println("============Accumulator: " + accum.value());

        return age;
    }

}
