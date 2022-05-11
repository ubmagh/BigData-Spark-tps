package temperaturesStreaming.accumulators;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;


public class Top5StationsAccumulator {
    private static volatile CollectionAccumulator<Tuple2<Double, String>> maxTmpStations = null;
    private static volatile CollectionAccumulator<Tuple2<Double, String>> minTmpStations = null;

    public static CollectionAccumulator<Tuple2<Double, String>> getTMaxTmpStationsInstance(JavaSparkContext jsc) {
        if (maxTmpStations == null) {
            synchronized (Top5StationsAccumulator.class) {
                if (maxTmpStations == null) {
                    maxTmpStations = jsc.sc().collectionAccumulator("maxTmpStations");
                }
            }
        }
        return maxTmpStations;
    }


    public static CollectionAccumulator<Tuple2<Double, String>> getTMinTmpStations(JavaSparkContext jsc) {
        if (minTmpStations == null) {
            synchronized (Top5StationsAccumulator.class) {
                if (minTmpStations == null) {
                    minTmpStations = jsc.sc().collectionAccumulator("minTmpStations");
                }
            }
        }
        return minTmpStations;
    }

}
