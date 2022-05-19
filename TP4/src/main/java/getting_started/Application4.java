package getting_started;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

public class Application4 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();

        Encoder<Employe> employeEncoder= Encoders.bean(Employe.class);
        Dataset<Employe> ds=ss.read().option("multiLine",true).json("employes.json").as(employeEncoder);
        ds.filter((FilterFunction<Employe>) emp->emp.getAge()>=30&&emp.getAge()<=35 && emp.getName().startsWith("A")).show();
        //ds.filter("salary > 10000 and name like 'A%'").show();

        ds.printSchema();

    }
}
