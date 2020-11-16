package springboot.test.javaspark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.List;

public class SparkSQLDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "/");
		// Create Spark Session to create connection to Spark

		//??????
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("Spark SQL Demo")
				.master("local")
				.getOrCreate();

		// A JSON dataset is pointed to by path.
		// The path can be either a single text file or a directory storing text files
			Dataset<Row> people = sparkSession
					.read()
					.option("multiLine", true)
					.json("src/main/resources/people.json");

			System.out.println("=============== test");

		// The inferred schema can be visualized using the printSchema() method
		people.printSchema();

		// Creates a temporary view using the DataFrame
		people.createOrReplaceTempView("people");

		// Alternatively, a DataFrame can be created for a JSON dataset represented by
		// a Dataset<String> storing one JSON object per string.
		List<String> jsonData = Arrays.asList(
				"{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		Dataset<String> anotherPeopleDataset = sparkSession.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> anotherPeople = sparkSession.read().json(anotherPeopleDataset);
		anotherPeople.show();

		System.out.println("=== Test filter ======");
		Dataset<Row> peopleDataset  = people.filter(people.col("age").gt(30))
				.withColumn("name", people.col("test"))
				.drop("test");
		peopleDataset.show();

		// Creates a temporary view using the DataFrame
		peopleDataset.createOrReplaceTempView("people_data_set");
		Dataset<Row> test = sparkSession.sql("SELECT name FROM people_data_set");
		test.show();
		people.show();

		Dataset<Row> select = sparkSession.sql("SELECT 2 * 10 AS total").toDF();
		select.show();
	}
}