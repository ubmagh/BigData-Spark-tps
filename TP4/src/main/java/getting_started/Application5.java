package getting_started;

import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.max;

public class Application5 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_CATALOGUE");
        options.put("user","root");
        options.put("password","");

        Dataset<Row> dfProduits=ss.read().
                format("jdbc")
                .options(options)
                .option("dbtable","PRODUITS")
                //.option("query","select P.NOM,P.PRIX from PRODUITS P,CATEORIE C where P.ID_CATEGORIE=C.ID_CAT and C.NOM='CAT1'")
                .load();
        dfProduits.show();
        Dataset<Row> dfCategory=ss.read().format("jdbc").options(options).option("dbtable","CATEORIE").load();

        dfCategory.show();
    }
}
