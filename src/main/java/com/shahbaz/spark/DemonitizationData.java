package com.shahbaz.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DemonitizationData {

    public void doAnalysis() {

        SparkSession sparkSession = new SparkSession.Builder()
                .appName("Demonitization Tweets")
                .master("local")
                .getOrCreate();

        Dataset<Row> dataframe = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("multiline",true)
                .option("sep",",")
                .option("quotes","\"")
                .option("inferSchema",true)
                .load("/Users/shahbaz/Downloads/Demonetization_data29th.csv");

        dataframe = dataframe.drop("QUERY","TWEET_ID","INSERTED DATE","TRUNCATED","LANGUAGE","possibly_sensitive","coordinates","retweeted_status","created_at_text","created_at","CONTENT","from_user_id","from_user_followers_count","from_user_friends_count","from_user_listed_count","from_user_statuses_count","from_user_description","from_user_location","from_user_created_at","retweet_count","entities_urls","entities_urls_counts","entities_mentions","entities_mentions_counts","in_reply_to_screen_name","in_reply_to_status_id","source","entities_expanded_urls","entities_media_count","media_expanded_url","media_url","media_type","video_link","photo_link","twitpic");

        dataframe.show(5, 150);

        dataframe.printSchema();


        //save in database
        String dbConnectionUrl = "jdbc:h2:file:~/demotDB";
        Properties properties = new Properties();
        properties.setProperty("driver","org.h2.Driver");
        properties.setProperty("user","sa");
        properties.setProperty("password","");

        dataframe.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl,"demotDB", properties);

    }

}
