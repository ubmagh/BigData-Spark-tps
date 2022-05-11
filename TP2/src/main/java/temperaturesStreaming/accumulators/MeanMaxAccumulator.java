package temperaturesStreaming.accumulators;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;


public class MeanMaxAccumulator {
    private static volatile LongAccumulator tmax_count = null;
    private static volatile DoubleAccumulator tmax_sum = null;

    public static LongAccumulator getCountInstance(JavaSparkContext jsc) {
        if (tmax_count == null) {
            synchronized (MeanMaxAccumulator.class) {
                if (tmax_count == null) {
                    tmax_count = jsc.sc().longAccumulator("tmax_count");
                }
            }
        }
        return tmax_count;
    }


    public static DoubleAccumulator getSumInstance(JavaSparkContext jsc) {
        if (tmax_sum == null) {
            synchronized (MeanMaxAccumulator.class) {
                if (tmax_sum == null) {
                    tmax_sum = jsc.sc().doubleAccumulator("tmax_sum");
                }
            }
        }
        return tmax_sum;
    }

}
