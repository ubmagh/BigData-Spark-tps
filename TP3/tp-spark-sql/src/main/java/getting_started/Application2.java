package getting_started;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Application2 {

    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder().appName("TP Spark SQL").master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().option("header", true).csv("students.csv");

        df.orderBy(col("note")).show();

        df.printSchema();

        df.select(col("note").cast("double"), col("id").cast("long")).printSchema();

    }

}
