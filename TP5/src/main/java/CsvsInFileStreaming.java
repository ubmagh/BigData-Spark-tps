import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class CsvsInFileStreaming {

    public static void main(String[] args) throws Exception {


        //   processing every new csv file, that appears inside './input' directory.

        SparkSession ss = SparkSession.builder()
                .appName("TP : Structured straming -> Csvs files in streamed folder :/-- ")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfLines = ss.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9090").load();


        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.LongType, false, Metadata.empty()),
                new StructField("phone", DataTypes.StringType, false, Metadata.empty()),
                new StructField("salary", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("departement", DataTypes.StringType, false, Metadata.empty()),
        });
        Dataset<Row> lines = ss
                .readStream()
                .option("header", true)
                .schema(schema)
                .csv("input");
        StreamingQuery query = lines.writeStream()
                .outputMode("append")
                .format("console")
                .start();
        query.awaitTermination();

    }
}
