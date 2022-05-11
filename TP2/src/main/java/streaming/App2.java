package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class App2 {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("TP2 : spark hdfs streaming").setMaster("local[*]");

        JavaStreamingContext jsp =  new JavaStreamingContext( conf, Durations.minutes(1) );
        JavaDStream<String> lines = jsp.textFileStream("hdfs://localhost:9000/words/");
        // whenever a new file is created in this directory, processing starts.

        JavaDStream<String> words = lines.flatMap( l -> Arrays.asList(l.split(" ")).iterator() );

        JavaPairDStream<String, Integer> wordsCount = words.mapToPair( s -> new Tuple2<>( s, 1))
                .reduceByKey(Integer::sum)
                ;

        wordsCount.print();

        jsp.start();
        jsp.awaitTermination();

    }
}
