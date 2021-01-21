package springboot.test.javaspark.sparksubmit;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class MySparkLauncher {
    public static void main(String[] args) throws IOException, InterruptedException {
        Process spark = new SparkLauncher()
                .setAppResource("target/java-spark-0.0.1-SNAPSHOT.jar")
                .setMainClass("src/main/java/springboot/test/javaspark/JavaSparkApplication.java")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .launch();

        spark.waitFor();
    }
}
