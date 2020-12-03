package springboot.test.javaspark.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import springboot.test.javaspark.model.Person;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        //===== Sort
        dfDataContent.sort(dfDataContent.col("age").desc()).show();

        //======= Filter dataFrame
        dfDataContent = dfDataContent.filter(dfDataContent.col("age").gt(20));
        dfDataContent.show();

        //======= Convert dataFrame to json
        var age = dfDataContent.toJSON().collectAsList().toString();
        System.out.println("=========== Age: " + age);


        dfDataContent.createOrReplaceTempView("data_content");
        var dfMaxAge = dfDataContent.select("*").toDF();
        dfMaxAge.show();
        var dfDataContentMaxAge = dfDataContent.select(max(dfDataContent.col("age")).alias("age"));
        dfDataContentMaxAge.show();

        //===== Expr
        dfDataContent = dfDataContent.withColumn("name_id", expr("name"));
        dfDataContent.show();


        //========== Test JavaRDD
//        JavaRDD<String> lines = javaSparkContext.textFile("src/main/resources/wordcount.txt");
//        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
//        System.out.println("======== lineLengths " + lineLengths);
//        int totalLength = lineLengths.reduce((a, b) -> a + b);
//        System.out.println("============ totalLength " + totalLength);

//        var dfDataContentMaxAgeRow = dfDataContent.withColumn("age", dfDataContent.col("age"))
//                                     .withColumn("name", dfDataContent.col("name"))
//                                     .where(max(dfDataContent.col("age")))
//                                     .toDF();
//
//        dfDataContentMaxAgeRow.show();


        //========= Accumulator
//        LongAccumulator accum = javaSparkContext.sc().longAccumulator();
//        javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> {
//            accum.add(x);
//        });
//        accum.value();
//        // returns 10
//        System.out.println("============Accumulator: " + accum.value());

        return age;
    }

    public String testSparkDF() throws ClassNotFoundException {
        Dataset<Row> peopleDataset = sparkSession.read().option("multiLine", true)
                                                    .json("src/main/resources/people.json");
        peopleDataset.show();

        Dataset<Row> dataDataset = sparkSession.read().option("multiLine", true)
                                .json("src/main/resources/data.json");
        dataDataset.show();

        //Join 2 table
        Dataset<Row> joinPeopleDataDataset = peopleDataset.
                join(dataDataset, dataDataset.col(";").equalTo(peopleDataset.col("people_id")))
                .select("id","name");
        joinPeopleDataDataset.show();

        //Create data frame with class
        Dataset<Row> personClassDataset= sparkSession.createDataFrame(Arrays.asList(
                new Person(1,"Data1",10),
                new Person(2,"Data2",10),
                new Person(3,"Data3",20),
                new Person(4,"Data4",22)
        ), Person.class);
        personClassDataset.show();

        //Drop field
        personClassDataset.drop("age").show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = sparkSession.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]
        transformedDS.show();

        //======= Load data from db
//        Class.forName("com.mysql.jdbc.Driver");
//        String url = "jdbc:mysql://192.168.2.4:3306/skypoint_db?user=root;password=";
//        Dataset<Row> df = sparkSession
//                .read()
//                .format("jdbc")
//                .option("url", url)
//                .option("dbtable", "account")
//                .load();
//        df.show();

        LongAccumulator accum = javaSparkContext.sc().longAccumulator();
        javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
        accum.value();
        System.out.println("============ Accumulator: " + accum.value());
        return  "Hello spark DF";
    }

    public String dataFrameJoin() {
        Dataset<Row> peopleDataset = sparkSession.read().option("multiLine", true)
                .json("src/main/resources/people.json");
        Dataset<Row> dataDataset = sparkSession.read().option("multiLine", true)
                .json("src/main/resources/data.json");

        //======== Join df and drop one of duplicate column
        Dataset<Row> joinDf = peopleDataset.join(dataDataset, peopleDataset.col("people_id").equalTo(
                dataDataset.col("id")), "inner")
                .drop(dataDataset.col("name"));
        joinDf.show();

        //======== Read json
        Dataset<Row> shipmentDf = sparkSession.read()
                .option("multiLine", true)
                .json("src/main/resources/shipment.json");

        //======= Make it 3DNF
        shipmentDf = shipmentDf
                .withColumn("supplierName", shipmentDf.col("supplier.name"))
                .withColumn("supplierCity", shipmentDf.col("supplier.city"))
                .withColumn("supplierCountry", shipmentDf.col("supplier.country"))
                .drop("supplier")
                .withColumn("items", explode(shipmentDf.col("books")))
                .drop("books");

        shipmentDf = shipmentDf
                .withColumn("itemTitle", shipmentDf.col("items.title"))
                .withColumn("itemQty", shipmentDf.col("items.qty"))
                .drop("items");
        shipmentDf.show();

        shipmentDf.createOrReplaceTempView("shipment_detail");
        Dataset<Row> bookCountDf = sparkSession.sql("SELECT count(*) AS titlCount FROM shipment_detail");
        bookCountDf.show();

        //====== NestedJoin
        //Dataset<Row> nestedJoin = functions.nest

        return "Hello join data frame";
    }

}

