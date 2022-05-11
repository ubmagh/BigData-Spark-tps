package TP;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Application {
    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder().appName("TP Spark SQL").master("local[*]").getOrCreate();

        // using JSON file
        // Dataset<Row> df = ss.read().option("multiline", true).json("employes.json");

        // using CSV file
        Dataset<Row> df = ss.read().option("header", true).csv("employes.csv");


        System.out.println("Employees with age between 30 & 35");
        df.select(
                col("id"),
                col("name"),
                col("departement"),
                col("age").cast("bigint")
                ).where(col("age").geq(30).and(col("age").leq(35))).show();


        System.out.println("Salary mean of each departement : ");
        df.select(
                col("departement"),
                col("salary").cast("double")
        ).groupBy(col("departement")).mean("salary")
                .show();


        System.out.println(" Emplooyees per departement : ");
        df.select(
                        col("departement")
                ).groupBy(col("departement")).count()
                .show();


        System.out.println(" Maximum salary per departement : ");
        df.select(
                        col("departement"),
                        col("salary").cast("double")
                ).groupBy(col("departement")).max("salary")
                .show();


    }

}
