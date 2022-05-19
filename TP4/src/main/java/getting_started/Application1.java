package getting_started;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application1 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();

        Dataset<Row> df=ss.read().option("multiLine",true).json("employes.json");
        df.printSchema();
        //df.show();
       //df.select("name","note").show();
        //df.select(col("name"),col("note").plus(1)).show();
       //df.filter(col("salary").gt(10000).and(col("name").startsWith("A"))).show();
       df.filter("salary > 10000 and name like 'A%'").show();
         df.createOrReplaceTempView("employes");
         ss.sql("select * from employes where salary > 10000 ").show();

    }
}
