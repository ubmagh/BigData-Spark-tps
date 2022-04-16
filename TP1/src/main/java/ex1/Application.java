package ex1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;

public class Application {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("EX1 TP1 RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // any random operations that respect the question

        // 1-> parallelize
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
                "Ayoub Jhon Doe Hamid Flap Jack Gumball ed ahmed ali khadija Youssef ibrahim issam mohamed "));

        // 2-> FlatMap
        JavaRDD<String> rdd2=rdd1.flatMap(s->Arrays.asList(s.split(" ")).iterator());

        // 3=> 3xfilter
        JavaRDD<String> rdd3 = rdd2.filter(a -> a.contains("a"));
        JavaRDD<String> rdd4 = rdd2.filter(a -> a.contains("b"));
        JavaRDD<String> rdd5 = rdd2.filter(a -> a.contains("c"));

        // 4 => union : rdd3 +rdd4
        JavaRDD<String> rdd6 = rdd3.union(rdd4);

        // 5 : map :
        // rdd5 => rdd71
        // rdd6 => rdd81
        JavaPairRDD<String,Integer>rdd71 = rdd5.mapToPair(s -> new Tuple2<>(s.toLowerCase(Locale.ROOT),1));
        JavaPairRDD<String,Integer> rdd81 = rdd6.mapToPair(s -> new Tuple2<>(s.toLowerCase(Locale.ROOT),1));

        // 6 :  reduce by key
        // rdd71 => rdd7
        // rdd81 => rdd8
        JavaPairRDD<String,Integer> rdd7 = rdd71.reduceByKey( (k1,k2) -> k1+k2 );
        JavaPairRDD<String,Integer> rdd8 = rdd81.reduceByKey((k1,k2) -> k1+k2);

        // 7 : union
        // rdd9 = rdd7 + rdd8
        JavaPairRDD<String,Integer> rdd9 = rdd8.union(rdd7);

        // 8 : sort
        JavaPairRDD<String,Integer> rdd10 = rdd9.sortByKey();


        //affichqge de RDD10
        rdd10.foreach(a -> System.out.println(a));
        System.out.println(rdd10.count());
    }
}
