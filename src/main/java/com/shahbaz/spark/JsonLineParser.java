package com.shahbaz.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonLineParser {

    public void printJsonSchema(){

        SparkSession sparkSession = new SparkSession.Builder()
                .appName("JSON parser")
                .master("local")
                .getOrCreate();

        Dataset<Row> dataFrame = sparkSession.read()
                .format("json")
                .option("multiline",true)
                .load("src/main/resources/multilineJson.json");

        dataFrame.show(2,200);
        dataFrame.printSchema();
    }
}
