package springboot.test.javaspark.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.crypto.Data;
import java.sql.Array;
import java.util.List;
import java.util.Map;

@Service
public class WordCountService {
 
    @Autowired
    JavaSparkContext sc;

    @Autowired
    private SparkSession sparkSession;


    public Map<String, Long> getCount(List<String> wordList) {
//        SparkSession sparkSession = SparkSession
//                .builder()
//                .appName("Spark SQL Demo")
//                .master("local")
//                .getOrCreate();
       // SparkSession spark = sparkSession.newSession();

//        Dataset<String> dataset = sparkSession.read().textFile("src/main/resources/wordcount.txt");
//        dataset.show();
//
//        Dataset<Row> people = sparkSession
//                .read()
//                .option("multiLine", true)
//                .json("src/main/resources/people.json");
//        people.show();
//
//
//        Dataset<Row> data = sparkSession
//                .read()
//                .option("multiLine", true)
//                .json("src/main/resources/people.json");
//        data.show();
//
//        data = data.filter(data.col("hobbies").isNotNull()).drop("test");
//        data.show();

        Dataset<Row> flightDataset = sparkSession.read().option("multiLine", true).json("src/main/resources/flight.json");
        flightDataset.show();

        Dataset<Row> fareComponentDescs = flightDataset.select("groupedItineraryResponse.fareComponentDescs")
                .drop("groupedItineraryResponse");
        fareComponentDescs.show();
        System.out.println("========== count " + fareComponentDescs.count());

       String test = fareComponentDescs.select("fareComponentDescs").rdd().collect().toString();
       System.out.println("Array " + test);


        JavaRDD<String> words = sc.parallelize(wordList);
        Map<String, Long> wordCounts = words.countByValue();
        return wordCounts;
    }

 
}