package by.dma;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import static org.apache.spark.sql.functions.md5;

public class Hashing {

    public void execute() {
        System.out.println("STARTING testMaxLength...");

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        String[][] data = {
                {"manufacturerId", "productId", "fieldKe1", "fieldLabel"},
                {"manufacturerId", null, "fieldKey", "fieldLabel"},
                {"manufacturerId", null, "fieldKey", null},
                {"manufacturerId", "productId", "fieldKe1", null}
        };

        StructType schema = new StructType(new StructField[]{
                new StructField("manufacturerId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("productId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("fieldKey", DataTypes.StringType, false, Metadata.empty()),
                new StructField("fieldLabel", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(
                Arrays.stream(data).sequential().map(RowFactory::create).toList(),
                schema
        );

        String[] columns = {"manufacturerId", "productId", "fieldKey", "fieldLabel"};
        Column[] columnObjects = Arrays.stream(columns).map(df::col).toArray(Column[]::new);

        UserDefinedFunction uuidFromMD5 = functions.udf((String s) -> {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] hashBytes = md.digest(s.getBytes(StandardCharsets.UTF_8));
                long msb = 0;
                long lsb = 0;
                for (int i = 0; i < 8; i++) {
                    msb = (msb << 8) | (hashBytes[i] & 0xff);
                }
                for (int i = 8; i < 16; i++) {
                    lsb = (lsb << 8) | (hashBytes[i] & 0xff);
                }
                return new UUID(msb, lsb).toString();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }, DataTypes.StringType);
        spark.udf().register("uuidFromMD5", uuidFromMD5);

        Dataset<Row> resultDF = df
                .withColumn("composedKey", functions.concat_ws("-", columnObjects))
                .withColumn("md5_hash", md5(functions.concat_ws("-", columnObjects)))
                .withColumn("uuid_from_md5", functions.callUDF("uuidFromMD5", functions.col("md5_hash")));

        resultDF.show(false);

        System.out.println("DONE");
    }

}