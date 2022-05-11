package getting_started.Application3;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class Application3 {

    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder().appName("TP Spark SQL-> DataSet<- ").master("local[*]").getOrCreate();

        Encoder<EmployeeBean> employeeBeanEncoder = Encoders.bean(EmployeeBean.class);
        Dataset<EmployeeBean> ds = ss.read().option("multiline", true).json("employes.json").as(employeeBeanEncoder);
        ds.printSchema();

        System.out.println(" ==> age between 30 & 35 : ");
        ds.filter((FilterFunction<EmployeeBean>) employeeBean -> employeeBean.getAge()>=30 && employeeBean.getAge()<=35).show();

    }

}
