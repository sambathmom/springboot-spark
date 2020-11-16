package springboot.test.javaspark.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
    public Map<String, Object> getData() {

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

//        String age = dfData.select("data.age").toString();
//        System.out.println("=========== Age: " + age);
        //======= Filter dataFrame
        dfDataContent = dfDataContent.filter(dfDataContent.col("age").gt(20));
        dfDataContent.show();

      //  dfDataContent = dfDataContent.select(max(dfDataContent.col("age")).alias("age"));
        dfDataContent = dfDataContent.withColumn("maxage", max(dfDataContent.col("age"))
                        .when(dfDataContent.col("age"), col("maxage")))
                        .toDF();
        dfDataContent.show();

        return null;
    }

}
