package temperaturesStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import temperaturesStreaming.accumulators.MaxMinTempsAccumulator;
import temperaturesStreaming.accumulators.MeanMaxAccumulator;
import temperaturesStreaming.accumulators.MeanMinAccumulator;
import temperaturesStreaming.accumulators.Top5StationsAccumulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Application {

    /*
    Same idea as TP1, this time I'm using stream Processing concept
     */

    public static void main(String[] args) throws InterruptedException {

        Logger log =Logger.getAnonymousLogger();

        SparkConf conf = new SparkConf().setAppName("TP2 : spark hdfs temperatures streaming").setMaster("local[*]"); // on local => using of threads

        JavaStreamingContext jsp =  new JavaStreamingContext( conf, Durations.minutes(1) );
        JavaDStream<String> lines = jsp.textFileStream("hdfs://localhost:9000/temps/");
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

        // max_count <--> max_sum # Mean of maximal
        max_count.foreachRDD((rdd, time) -> {
            LongAccumulator max_count_accumulator = MeanMaxAccumulator.getCountInstance(new JavaSparkContext(rdd.context()));
            long value = rdd.first();
            if( value!=0 ) {
                max_count_accumulator.setValue(value);
                log.info("[max_count]> time: " + time + "  -  value : " + value);
            }
        });
        max_sum.foreachRDD((rdd, time) -> {
            DoubleAccumulator max_sum_accumulator = MeanMaxAccumulator.getSumInstance(new JavaSparkContext(rdd.context()));
            LongAccumulator max_count_accumulator = MeanMaxAccumulator.getCountInstance(new JavaSparkContext(rdd.context()));
            if( !rdd.isEmpty() ) {
                double value = rdd.first();
                max_sum_accumulator.setValue(value);
                log.info("[max_total]> time: " + time + "  -  value : " + value);
                log.info(" [Mean of max temps]> " + value / max_count_accumulator.value());
            }
        });

        // min_count <--> min_sum
        min_count.foreachRDD((rdd, time) -> {
            LongAccumulator min_count_accumulator = MeanMinAccumulator.getCountInstance(new JavaSparkContext(rdd.context()));
            long value = rdd.first();
            if( value!=0 ) {
                min_count_accumulator.setValue(value);
                log.info("[min_count]> time: " + time.toString() + "  -  value : " + value);
            }
        });
        min_sum.foreachRDD((rdd, time) -> {
            DoubleAccumulator min_sum_accumulator = MeanMinAccumulator.getSumInstance(new JavaSparkContext(rdd.context()));
            LongAccumulator min_count_accumulator = MeanMinAccumulator.getCountInstance(new JavaSparkContext(rdd.context()));
            if( !rdd.isEmpty() ) {
                double value = rdd.first();
                min_sum_accumulator.setValue(value);
                log.info("[min_total]> time: " + time + "  -  value : " + value);
                log.info(" [Mean of min temps]> " + value / min_count_accumulator.value());
            }
        });


        // Max & Min temperatures
        JavaDStream<Double> max_TMAX = maximalTemperatures.reduce(Math::max),
                            min_TMIN = minimalTemperatures.reduce(Math::min);
        max_TMAX.foreachRDD((rdd, time) -> {
            DoubleAccumulator tmax_accumulator = MaxMinTempsAccumulator.getTMaxInstance(new JavaSparkContext(rdd.context()));
            if( !rdd.isEmpty() ) {
                double value = rdd.first();
                tmax_accumulator.setValue(value);
                log.info("[Max temperature]> time: " + time + "  -  value : " + value);
            }
        });
        min_TMIN.foreachRDD((rdd, time) -> {
            DoubleAccumulator tmin_accumulator = MaxMinTempsAccumulator.getTMinInstance(new JavaSparkContext(rdd.context()));
            if( !rdd.isEmpty() ) {
                double value = rdd.first();
                tmin_accumulator.setValue(value);
                log.info("[Min temperature]> time: " + time + "  -  value : " + value);
            }
        });


        // todo : TOP 5 stations with max and min temps ( sorting stations using the mean of TMIN & TMAX of each station)
        // There 's an issu down here :D

        JavaPairDStream<String, Iterable<StationRecord>> recordsPerStation = stations_records.mapToPair(stationRecord -> {
            return new Tuple2<>( stationRecord.getId(), stationRecord );
        }).groupByKey();

        JavaPairDStream< String, List<Double>> Tmax_recordsPerStation = recordsPerStation.mapToPair( tupleElem -> {
            List<StationRecord> list = new ArrayList<>();
            List<Double> listD = new ArrayList<>();
            tupleElem._2.forEach(list::add);
            listD = list.stream().filter(stationRecord -> stationRecord.getRecordType().equals("TMAX")).map(StationRecord::getTemperature).collect(Collectors.toList());
            return new Tuple2<String, List<Double>>( tupleElem._1.toString(), listD);
        });

        JavaPairDStream< String, List<Double>> Tmin_recordsPerStation = recordsPerStation.mapToPair( tupleElem -> {
            List<StationRecord> list = new ArrayList<>();
            List<Double> listD = new ArrayList<>();
            tupleElem._2.forEach(list::add);
            listD = list.stream().filter(stationRecord -> stationRecord.getRecordType().equals("TMIN")).map(StationRecord::getTemperature).collect(Collectors.toList());
            return new Tuple2<String, List<Double>>( tupleElem._1.toString(), listD);
        });

        JavaPairDStream< Double, String> stationsMeanTmax =  Tmax_recordsPerStation.mapToPair(tuple -> {
            double mean=0;
            if(tuple._2.size()>0)
                mean = tuple._2.stream().reduce( 0.,Double::sum)/tuple._2.size() ;
            else
                mean=0;
            return new Tuple2<>( mean, tuple._1);
        }); // sorting !!

        JavaPairDStream< Double, String> stationsMeanTmin =  Tmin_recordsPerStation.mapToPair(tuple -> {
            double mean=0;
            if(tuple._2.size()>0)
                mean = tuple._2.stream().reduce( 0.,Double::sum)/tuple._2.size() ;
            else
                mean=0;
            return new Tuple2<>( mean, tuple._1);
        }); // sorting !!


        stationsMeanTmax.foreachRDD((rdd, time) -> {
            CollectionAccumulator<Tuple2<Double, String>> topHotest = Top5StationsAccumulator.getTMaxTmpStationsInstance(new JavaSparkContext(rdd.context()));
            if( !rdd.isEmpty() ) {
                rdd.sortByKey(false);
                rdd.foreach(topHotest::add);
            }else{
                List<Tuple2<Double, String>> tuple2List = topHotest.value();
                tuple2List = tuple2List.stream().sorted(Comparator.comparingDouble(t -> t._1)).collect(Collectors.toList());
                List<Tuple2<Double, String>> list = tuple2List.subList(0,4) ;
                log.info("[Top 5 HOTest stations ]=> " +list);
            }
        });

        stationsMeanTmin.foreachRDD((rdd, time) -> {
            if( !rdd.isEmpty() ) {
                rdd.sortByKey(true);
                List<Tuple2<Double, String>> list = rdd.take(5);
                log.info("[Top 5 Coldest stations ]=> " +list);
            }
        });


        jsp.start();
        jsp.awaitTermination();
    }
}