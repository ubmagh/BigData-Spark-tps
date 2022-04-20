package ex3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class Application1 {

    private static int YEAR = 1750;


    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("EX3 TP1 RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1=sc.textFile(YEAR+".csv");

        JavaRDD<StationRecord> stations_records = rdd1.map(s -> {
            List<String> splitted  =Arrays.asList(s.split(",") ) ;
            StationRecord sr = new StationRecord( splitted.get(0), splitted.get(2), Double.parseDouble( splitted.get(3) ), splitted.get(1));
            return sr;
        });

        JavaRDD<StationRecord> tmaxRDD = stations_records.filter(
                sr -> sr.getRecordType().equals("TMAX")
        );

        JavaRDD<StationRecord> tminRDD = stations_records.filter(
                sr -> sr.getRecordType().equals("TMIN")
        );

        JavaRDD<Double> minimalTemperatures = tminRDD.map(sr -> sr.getTemperature());

        JavaRDD<Double> maximalTemperatures = tmaxRDD.map(StationRecord::getTemperature);

        long min_count = minimalTemperatures.count(),
            max_count = maximalTemperatures.count()
        ;
        double min_sum = minimalTemperatures.reduce(Double::sum),
             max_sum = maximalTemperatures.reduce(Double::sum)
         ;

        double max_TMAX = maximalTemperatures.reduce(Math::max);
        double min_TMIN = minimalTemperatures.reduce(Math::min);

        System.out.println(" ==> Température minimale moyenne : "+min_sum/min_count);
        System.out.println(" ==> Température maximal moyenne : "+max_sum/max_count);
        System.out.println(" ==> Température maximal la plus elevée : "+max_TMAX);
        System.out.println(" ==> Température minimal la plus basse : "+min_TMIN);

        JavaPairRDD<String, Iterable<StationRecord>> recordsPerStation = stations_records.mapToPair(stationRecord -> {
            return new Tuple2<>( stationRecord.getId(), stationRecord );
        }).groupByKey();

        JavaPairRDD< String, List<Double>> Tmax_recordsPerStation = recordsPerStation.mapToPair( tupleElem -> {
            List<StationRecord> list = new ArrayList<>();
            List<Double> listD = new ArrayList<>();
            tupleElem._2.forEach(list::add);
            listD = list.stream().filter(stationRecord -> stationRecord.getRecordType().equals("TMAX")).map(StationRecord::getTemperature).collect(Collectors.toList());
            return new Tuple2<String, List<Double>>( tupleElem._1.toString(), listD);
        });

        JavaPairRDD< String, List<Double>> Tmin_recordsPerStation = recordsPerStation.mapToPair( tupleElem -> {
            List<StationRecord> list = new ArrayList<>();
            List<Double> listD = new ArrayList<>();
            tupleElem._2.forEach(list::add);
            listD = list.stream().filter(stationRecord -> stationRecord.getRecordType().equals("TMIN")).map(StationRecord::getTemperature).collect(Collectors.toList());
            return new Tuple2<String, List<Double>>( tupleElem._1.toString(), listD);
        });

        JavaPairRDD< Double, String> stationsMeanTmax =  Tmax_recordsPerStation.mapToPair(tuple -> {
            double mean=0;
            if(tuple._2.size()>0)
                mean = tuple._2.stream().reduce( 0.,Double::sum)/tuple._2.size() ;
            else
                mean=0;
            return new Tuple2<>( mean, tuple._1);
        }).sortByKey(false);

        JavaPairRDD< Double, String> stationsMeanTmin =  Tmin_recordsPerStation.mapToPair(tuple -> {
            double mean=0;
            if(tuple._2.size()>0)
                mean = tuple._2.stream().reduce( 0.,Double::sum)/tuple._2.size() ;
            else
                mean=0;
            return new Tuple2<>( mean, tuple._1);
        }).sortByKey(true);

        List<Tuple2<Double,String>> listTop5Chaudes = stationsMeanTmax.take(5);
        List<Tuple2<Double,String>> listTop5Froides = stationsMeanTmin.take(5);

        System.out.println(" ==> Top 5 des stations météo les plus chaudes : "+listTop5Chaudes);
        System.out.println("        --> selon les moyens des températures max TMAX ");
        System.out.println(" ==> Top 5 des stations météo les plus froides : "+listTop5Froides);
        System.out.println("        --> selon les moyens des températures min TMIN ");

    }

}
