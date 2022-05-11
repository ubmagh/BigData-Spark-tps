package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

public class App {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("TP2 : spark tcp streaming").setMaster("local[*]");

        JavaStreamingContext jsp =  new JavaStreamingContext( conf, Durations.minutes(2) );
        JavaReceiverInputDStream<String> lines = jsp.socketTextStream( "localhost", 9090);
        JavaDStream<String> words = lines.flatMap( l -> Arrays.asList(l.split(" ")).iterator() );

        JavaPairDStream<String, Integer> wordsCount = words.mapToPair( s -> new Tuple2<>( s, 1))
                .reduceByKey(Integer::sum)
                ;

        wordsCount.print();
        jsp.start();
        jsp.awaitTermination();

    }
}
