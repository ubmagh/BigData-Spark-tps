package ex2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("EX2-Q2 TP1 RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1=sc.textFile("ventes.txt");
        JavaPairRDD<String,Double> rdd2=rdd1.mapToPair(s ->
                new Tuple2<>(
                    Arrays.asList(s.split(" ")).get(0)+","+Arrays.asList(s.split(" ")).get(1)
                    ,Double.parseDouble( Arrays.asList(s.split(" ")).get(3) )
                )
            );


        System.out.println("++> Total des ventes ; ");
        JavaPairRDD<String, Double> rdd3 = rdd2.reduceByKey((aDouble, aDouble2) -> aDouble+aDouble2);
        rdd3.foreach(stringDoubleTuple2 -> System.out.println(stringDoubleTuple2));
    }

}
