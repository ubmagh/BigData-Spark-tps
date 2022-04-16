package getting_started;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class Application1 {
    public static void main(String[] args) {
        SparkConf  conf=new SparkConf().setAppName("word count").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.textFile("names.txt");
        JavaRDD<String> rdd2=rdd1.flatMap(s->Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String,Integer> rdd3=rdd2.mapToPair(s->new Tuple2<>(s,1));
        JavaPairRDD<String,Integer> rdd4=rdd3.reduceByKey((v1, v2)->v1+v2);
        rdd4.foreach(nameTuple-> System.out.println(nameTuple._1()+" "+nameTuple._2()));
    }
}
