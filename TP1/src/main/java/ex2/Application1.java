package ex2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Application1 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("EX2-Q1 TP1 RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1=sc.textFile("ventes.txt");
        JavaRDD<Double> rdd2=rdd1.map( s -> Double.parseDouble( Arrays.asList(s.split(" ")).get(3) ) );

        System.out.println("++> Total des ventes ; "+rdd2.reduce(Double::sum));
    }

}
