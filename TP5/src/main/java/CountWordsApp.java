import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class CountWordsApp {

        public static void main(String[] args) throws TimeoutException, StreamingQueryException {


                SparkSession ss = SparkSession.builder()
                        .appName("TP : Structured straming word count ")
                        .master("local[*]")
                        .getOrCreate();

                Dataset<Row> dfLines = ss.readStream()
                        .format("socket")
                        .option("host","localhost")
                        .option("port", "9090").load();

                Dataset<String> dsWords = dfLines.as(Encoders.STRING())
                        .flatMap( (FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING() );

                Dataset<Row> dfWordCount = dsWords.groupBy("value").count();

                // try the 3 output mods
                StreamingQuery query = dfWordCount.writeStream().outputMode(OutputMode.Complete()).format("console").start();


                // before running this app, run a socket server on configured port with the command : $> nc -lc 9090
                query.awaitTermination();



        }

}
