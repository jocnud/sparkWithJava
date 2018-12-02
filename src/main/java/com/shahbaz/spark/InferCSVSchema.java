package com.shahbaz.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {

    public void printSchema(){
        SparkSession sparkSession = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        //load data into dataset
        Dataset<Row> dataframe = sparkSession.read().format("csv")
                .option("header", "true")
                .option("multiline",true)
                .option("sep",";")
                .option("quote","^")
                .option("dateFormat","M/d/y")
                .option("inferSchema",true)
                .load("src/main/resources/amazonProducts.txt");

        dataframe.show(100,90);
        dataframe.printSchema();

    }
}
