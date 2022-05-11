package TP.dataSet;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class Application {
    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder().appName("TP Spark SQL").master("local[*]").getOrCreate();
        Encoder<EmployeeBean> employeeBeanEncoder = Encoders.bean(EmployeeBean.class);

        // using JSON file
        Dataset<EmployeeBean> ds = ss.read().option("multiline", true).json("employes.json").as(employeeBeanEncoder);

        // using CSV file
        //Dataset<EmployeeBean> ds = ss.read().format("csv").option("delimiter",",").option("header", true).option("charset", "UTF8") .option("inferSchema", "true").csv("employes.csv").as(employeeBeanEncoder);

        System.out.println("Employees with age between 30 & 35");
        ds.filter((FilterFunction<EmployeeBean>) employeeBean -> employeeBean.getAge()>=30 && employeeBean.getAge()<=35).show();

        System.out.println("Salary mean of each departement : ");
        ds.select(
                col("departement"),
                col("salary")
        ).groupBy(col("departement")).mean("salary")
                .show();


        System.out.println(" Emplooyees per departement : ");
        ds.select(
                        col("departement")
                ).groupBy(col("departement")).count()
                .show();


        System.out.println(" Maximum salary per departement : ");
        ds.select(
                        col("departement"),
                        col("salary")
                ).groupBy(col("departement")).max("salary")
                .show();


    }

}
