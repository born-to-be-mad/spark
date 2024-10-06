package by.dma;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class SparkBaseDemo {
    private static final String CSV_URL = "movies.csv";

    public void execute() {
        System.out.println("STARTING...");
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> csv = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(CSV_URL);
        csv.show();
        csv.printSchema();

        System.out.println("#########################");
        System.out.println("###   RUNTIME > 100   ###");
        csv.filter("runtime > 100").show();

        System.out.println("########################");
        System.out.println("### TITLE like '%B%' ###");
        csv.filter("title like '%B%'").show();

        System.out.println("#######################");
        System.out.println("### Movies by years ###");
        csv.groupBy(col("year"))
                .avg("runtime").as("avg.runtime")
                .show();

        System.out.println("#########################################");
        System.out.println("### Movies by years with average time ###");
        csv.groupBy(col("year"))
                .count()
                .show();

        System.out.println("############################");
        System.out.println("### Restriction by genre ###");
        String customFunction = "customFunction";
        spark.udf().register(customFunction, getGenreRestriction, DataTypes.StringType);
        csv.withColumn("restriction", callUDF(customFunction, col("genres"))).show();

        System.out.println("DONE");
    }

    private static final UDF1 getGenreRestriction = (UDF1<String, String>) genres -> {
        if (genres.contains("Horror") || genres.contains("Thriller") || genres.contains("Crime")) {
            return "18+";
        }
        if (genres.contains("Anumation") || genres.contains("Fantasy") || genres.contains("Family")) {
            return "6++";
        }
        return "12+";
    };
}