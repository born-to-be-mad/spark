package by.dma;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkExperiments {
    private static final String CSV_URL = "movies.csv";

    public void testMaxLength() {
        System.out.println("STARTING testMaxLength...");

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        String[] data = {
                "-123",
                "qweqwer",
                "2.12332",
                null,
                "",
                "9223372036854775807",
                "92233720368547758123"
        };

        StructType schema = new StructType(new StructField[]{
                new StructField("INTEGER_VALUE", DataTypes.StringType, true, Metadata.empty())
        });

        // Create DataFrame
        Dataset<Row> df = spark.createDataFrame(
                Arrays.stream(data).sequential().map(RowFactory::create).toList(),
                schema
        );

        // Register UDF to check and parse MAX_CHAR_LIMIT
        spark.udf().register("validateInteger", (UDF1<String, String>) fieldLimit -> {
            if (fieldLimit != null) {
                try {
                    Integer.parseInt(fieldLimit);
                    return "Valid";  // No error
                } catch (NumberFormatException e) {
                    return "Invalid value: " + fieldLimit;
                }
            }
            return "Valid";  // No error if null
        }, DataTypes.StringType);

        // Apply UDF and filter rows with errors
        Dataset<Row> errorsDf = df.withColumn("error", functions.callUDF("validateInteger", col("MAX_CHAR_LIMIT")));
                //.filter(col("error").isNotNull()


        // Show errors
        errorsDf.show(false);

        // Collect errors into a list (if needed)
        List<String> errors = errorsDf.select("error").as(Encoders.STRING()).collectAsList();

        // Print errors
        for (String error : errors) {
            System.out.println(error);
        }
        System.out.println("DONE");
    }

    public void testQuotesReplacement() {
        System.out.println("STARTING testQuotesReplacement...");
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        String[] data = {
                "\"Start \"\" End\"",
                "Hidden Field \"\"Label 001",
                "\"Another \"\"Example\"\"\"",
                "\"Just a test\"",
                "\"Just \"a\" test\""
        };


        Dataset<Row> df = spark.createDataFrame(
                Arrays.stream(data).sequential().map(RowFactory::create).toList(),
                new StructType(new StructField[]{
                        new StructField("fieldLabel", DataTypes.StringType, false, Metadata.empty())
                })
        );
        df = df.withColumn("try1",
                regexp_replace(
                        regexp_replace(
                                regexp_replace(col("fieldLabel"),
                                        "^\"", ""),  // Remove " at the start
                                "\"\"", "\""),  // Replace "" with "
                        "\"$", "")  // Remove " at the end
        );

        df = df.withColumn("try2",
                regexp_replace(
                        regexp_replace(
                                regexp_replace(
                                        regexp_replace(col("fieldLabel"), "^\"", ""),  // Remove " at the start
                                        "\"\"", "\""),  // Replace "" with "
                                "\"$", ""),  // Remove " at the end
                        "\\\\\"", "\"")  // Replace \" with "
        );

        df.show(false);
        System.out.println("DONE");
    }
}