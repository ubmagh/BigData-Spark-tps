package getting_started;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Application3 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();
        //json
        //Dataset<Row> df=ss.read().option("multiLine",true).json("employes.json");
        //csv
        Dataset<Row> df=ss.read().option("header",true).csv("employes.csv");
        //df.printSchema();

        df.select(col("departement"),col("salary").cast("double"))
                .groupBy("departement").avg("salary").show();



    }
}
