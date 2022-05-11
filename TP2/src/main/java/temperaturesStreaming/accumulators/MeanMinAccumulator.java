package temperaturesStreaming.accumulators;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;


public class MeanMinAccumulator {
    private static volatile LongAccumulator tmin_count = null;
    private static volatile DoubleAccumulator tmin_sum = null;

    public static LongAccumulator getCountInstance(JavaSparkContext jsc) {
        if (tmin_count == null) {
            synchronized (MeanMinAccumulator.class) {
                if (tmin_count == null) {
                    tmin_count = jsc.sc().longAccumulator("tmin_count");
                }
            }
        }
        return tmin_count;
    }


    public static DoubleAccumulator getSumInstance(JavaSparkContext jsc) {
        if (tmin_sum == null) {
            synchronized (MeanMinAccumulator.class) {
                if (tmin_sum == null) {
                    tmin_sum = jsc.sc().doubleAccumulator("tmin_sum");
                }
            }
        }
        return tmin_sum;
    }

}
