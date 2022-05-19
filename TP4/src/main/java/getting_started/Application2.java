package getting_started;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.transform;

public class Application2 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();

        Dataset<Row> df=ss.read().option("header",true).
                option("inferSchema",true).csv("employes.csv");
        //convertir un datframe vers un rdd

        Encoder<Employe> employeEncoder= Encoders.bean(Employe.class);
        JavaRDD<Employe> rdd1=df.as(employeEncoder).javaRDD();
        //convertir un  rdd vers un  datframe
       Dataset<Row> df1=ss.createDataFrame(rdd1,Employe.class);
        df1.show();
        //convertir un  datframe vers un  dataset

        Dataset<Employe> dsEmployes=df1.map((MapFunction<Row,Employe>) row-> {
            Employe e=new Employe();
            e.setId(row.getLong(2));
            e.setName(row.getString(3));
            e.setAge(row.getLong(0));
            return  e;},employeEncoder);
      dsEmployes.show();
      //convertir un Dataset vers un RDD
      JavaRDD<Employe> rdd2=dsEmployes.javaRDD();

        //df.select(col("id").cast("long"),col("name"),col("note").cast("double")).printSchema();

    }
}
