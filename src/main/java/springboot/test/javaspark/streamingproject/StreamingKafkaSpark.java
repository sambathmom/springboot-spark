package springboot.test.javaspark.streamingproject;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

@Service
public class StreamingKafkaSpark {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("StreamingKafkaConsumer")
                .master("local")
                .getOrCreate();

        // Subscribe to 1 topic
        Dataset<Row> messagesDf = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load()
                .selectExpr("CAST(value AS STRING)");;
        //messagesDf = messagesDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        Dataset<String> words = messagesDf
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        query.awaitTermination();
    }
}
