import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;

/*
            configuration of my MySQL server hosted on Host machine
            inside of my.ini :
            $ bind-adress="0.0.0.0"
            and
            $ [mysqld]
            $ port=3306
            $ skip-grant-tables
         */

public class Solution {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://192.168.67.1:3306/vm");

        options.put("user","root");
        options.put("password","");


        // afficher les catégories de produits les plus achetées en terme de quantité
        // show list of categories that are most bought in term of quantity :/

        System.out.println("La liste des catégories de produits les plus achetées en terme de quantité :");
        Dataset<Row> top15BoughtCategories=ss.read().
                format("jdbc")
                .options(options)
                .option("query",
                        " SELECT c.*, SUM(i.order_item_quantity) as totalBoughtQte " +
                        " FROM categories c, products p, order_items i " +
                        " WHERE c.category_id = p.product_category_id " +
                        " AND p.product_id = i.order_item_product_id " +
                        " GROUP BY c.category_name " +
                        " ORDER BY totalBoughtQte DESC " +
                        " LIMIT 15")
                .load();
        top15BoughtCategories.show();



        // afficher les 5 produits les plus intéressants en chiffre de vente
        // show list of 5 products that are most sold  :/
        System.out.println("La liste des 5 produits les plus intéressants en chiffre de vente :");
        Dataset<Row> top5SoldProducts = ss.read().
                format("jdbc")
                .options(options)
                .option("query",
                        " SELECT p.*, SUM(i.order_item_subtotal) as totalSolde " +
                                " FROM  products p, order_items i " +
                                " WHERE p.product_id = i.order_item_product_id" +
                                " GROUP BY p.product_id " +
                                " ORDER BY totalSolde DESC " +
                                " LIMIT 5")
                .load();
        top5SoldProducts.show();


    }
}
