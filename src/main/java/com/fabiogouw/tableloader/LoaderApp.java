package com.fabiogouw.tableloader;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class LoaderApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("AWS Dynamo Sample")
                .getOrCreate();

        spark
                .sqlContext()
                .udf()
                .register( "createJson", createJson(), DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("amount", DataTypes.FloatType, false),
                                DataTypes.createStructField("date", DataTypes.StringType, false)
                        )));

        Dataset<Row> df = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load(args[0]);

        df = df.withColumn( "json",
                functions.callUDF( "createJson", df.col( "amount" ),
                        df.col( "date" )) );
        df = df.drop("amount").drop("date");
        df.printSchema();
        df.write()
                .format("dynamodb")
                .mode(SaveMode.Append)
                .option("tableName", "contracts")
                .option("writeBatchSize", 10)
                .option("targetCapacity", 1)
                .save();
        spark.stop();
    }

    public static UDF2<String, String, Row> createJson()
    {
        return ( s1, s2 ) -> {
            return RowFactory.create(Float.parseFloat(s1), s2);
        };
    }
}
