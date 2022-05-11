package temperaturesStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.DoubleAccumulator;
import temperaturesStreaming.accumulators.TmaxAccumulator;

import java.util.Arrays;
import java.util.List;

public class Application {

    /*
    Same idea as TP1, this time I'm using stream Processing concept
     */

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("TP2 : spark hdfs temperatures streaming").setMaster("local[*]"); // on local => using of threads

        JavaStreamingContext jsp =  new JavaStreamingContext( conf, Durations.seconds(10) );
        JavaDStream<String> lines = jsp.textFileStream("hdfs://localhost:9000/words/");
        // whenever a new file appears in this directory, processing starts.

        JavaDStream<StationRecord> stations_records = lines.map(s -> {
            List<String> splitted  =Arrays.asList(s.split(",") ) ;
            StationRecord sr = new StationRecord( splitted.get(0), splitted.get(2), Double.parseDouble( splitted.get(3) ), splitted.get(1));
            return sr;
        });

        JavaDStream<StationRecord> tmaxRDD = stations_records.filter(
                sr -> sr.getRecordType().equals("TMAX")
        );


        JavaDStream<StationRecord> tminRDD = stations_records.filter(
                sr -> sr.getRecordType().equals("TMIN")
        );

        JavaDStream<Double> minimalTemperatures = tminRDD.map(StationRecord::getTemperature);

        JavaDStream<Double> maximalTemperatures = tmaxRDD.map(StationRecord::getTemperature);

        JavaDStream<Long> min_count =  minimalTemperatures.count(),
                max_count = maximalTemperatures.count()
                        ;
        JavaDStream<Double>  min_sum = minimalTemperatures.reduce(Double::sum),
                max_sum = maximalTemperatures.reduce(Double::sum)
                        ;


        JavaDStream<Double> max_TMAX = maximalTemperatures.reduce(Math::max),
                            min_TMIN = minimalTemperatures.reduce(Math::min);







        jsp.start();
        jsp.awaitTermination();
    }
}