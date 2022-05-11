package temperaturesStreaming.accumulators;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;

public class TmaxAccumulator {
    private static volatile DoubleAccumulator tmax = null;
    public static DoubleAccumulator getInstance(JavaSparkContext jsc) {
        if (tmax == null) {
            synchronized (TmaxAccumulator.class) {
                if (tmax == null) {
                    tmax = jsc.sc().doubleAccumulator("TmaxAccumulator");
                }
            }
        }
        return tmax;
    }
}
