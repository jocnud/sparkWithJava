package com.shahbaz.spark;

import org.apache.spark.sql.*;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

/**
 *
 */
public class App {
    public static void main(String[] args) {
        //create a session
        SparkSession sparkSession = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        //load data into dataset
        Dataset<Row> dataframe = sparkSession.read().format("csv")
                .option("header", "true")
                .load("src/main/resources/name_and_comments.txt");

        // adding a new column in dataframe
        //note that dataframe is immutable so we need to re-assign it back to dataframe after transformation
        dataframe = dataframe.withColumn("full_name",
                concat(dataframe.col("last_name"), lit(", "), dataframe.col("first_name")));
        dataframe.show();


        // filtering column which has only numbers in the comments
        dataframe = dataframe.filter(dataframe.col("comment").rlike("\\d+"));
        dataframe.show();

        //order by
        dataframe = dataframe.orderBy(dataframe.col("last_name").asc());
        dataframe.show();

        //save in database
        String dbConnectionUrl = "jdbc:h2:file:~/testDB";
        Properties properties = new Properties();
        properties.setProperty("driver","org.h2.Driver");
        properties.setProperty("user","sa");
        properties.setProperty("password","");

        dataframe.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl,"testDB", properties);





    }
}
