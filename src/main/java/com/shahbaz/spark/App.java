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


        InferCSVSchema parser = new InferCSVSchema();
        parser.printSchema();

//        DefineSchema parser = new DefineSchema();
//        parser.printWithDefineSchema();

//        JsonLineParser jsonLineParser = new JsonLineParser();
//        jsonLineParser.printJsonSchema();

        DemonitizationData demonitizationData = new DemonitizationData();
        demonitizationData.doAnalysis();

    }
}
