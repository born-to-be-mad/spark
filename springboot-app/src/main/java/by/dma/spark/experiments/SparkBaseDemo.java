package by.dma.spark.experiments;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

        System.out.println("DONE");
    }
}
