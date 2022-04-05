package com.jincongho;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("App")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().format("com.jincongho").load();
        df.show();
        System.out.println("Number of partition in simple source is " + df.rdd().getNumPartitions());

        DataFrameWriter<Row> dfw = df.write().format("com.jincongho");
        dfw.save();
        System.out.println("Finish writing");
    }

}
