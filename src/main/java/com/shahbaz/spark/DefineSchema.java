package com.shahbaz.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

public class DefineSchema {

    public void printWithDefineSchema() {

        SparkSession sparkSession = new SparkSession.Builder()
                .appName("Complex CSV with a defined Schema")
                .master("local")
                .getOrCreate();

        StructType userDefinedSchema = createStructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("product_id", IntegerType, false),
                createStructField("title", StringType, false),
                createStructField("published_on", DateType, false),
                createStructField("url", StringType, false)
        });

        Dataset<Row> dataframe = sparkSession.read().format("csv")
                .option("header", "true")
                .option("multiline",true)
                .option("sep",";")
                .option("quote","^")
                .option("dateFormat","M/d/y")
               .schema(userDefinedSchema)
                .load("src/main/resources/amazonProducts.txt");

        dataframe.show();
        dataframe.printSchema();

    }
}
