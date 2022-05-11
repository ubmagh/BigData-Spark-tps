package temperaturesStreaming.accumulators;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;


public class MaxMinTempsAccumulator {
    private static volatile DoubleAccumulator tmax = null;
    private static volatile DoubleAccumulator tmin = null;

    public static DoubleAccumulator getTMinInstance(JavaSparkContext jsc) {
        if (tmin == null) {
            synchronized (MaxMinTempsAccumulator.class) {
                if (tmin == null) {
                    tmin = jsc.sc().doubleAccumulator("tmin");
                }
            }
        }
        return tmin;
    }


    public static DoubleAccumulator getTMaxInstance(JavaSparkContext jsc) {
        if (tmax == null) {
            synchronized (MaxMinTempsAccumulator.class) {
                if (tmax == null) {
                    tmax = jsc.sc().doubleAccumulator("tmax");
                }
            }
        }
        return tmax;
    }

}
