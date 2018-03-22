package com.scottlogic.blog.analysis;

import org.apache.hadoop.util.Time;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.year;

public class BikeDataAnalysis {

    private static final int TWELVE_HOURS_IN_SECONDS = 12 * 60 * 60;

    private static String file = "data/full-bike";

    public static void main(String[] args) {
        //localSparkJob();
        yarnSparkJob();
    }

    private static void localSparkJob() {
        SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("Spark Performance - Cycle Hire Analysis Bikes - MDB")
                .config("spark.eventLog.dir", "file:///c:/spark/logs")
                .config("spark.eventLog.enabled", true)
                .getOrCreate();

        repartitionWeekendPostParquet(spark);
        repartitionWeekendPreParquet(spark);
        spark.stop();

    }

    private static void yarnSparkJob() {
        SparkSession spark = SparkSession.builder()
                .master("yarn")
                .appName("Spark Performance - Cycle Hire Analysis Bikes - MDB")
                .config("spark.eventLog.enabled", true)
                .getOrCreate();

        repartitionWeekendPostParquet(spark);
        repartitionWeekendPreParquet(spark);
        spark.stop();
    }

    /**
     * Apply casting and filters to prepare the data for processing
     * - Cast Duration to Int
     * - Cast Start Date to Timestamp
     * - Add Weekday Column
     * - Filter Duration by greater than 0, less than 12 hours (in seconds) and not null
     * - Filter Start Date by greater than 2011 (some results are dated from years 1900 and 1901)
     *
     * @param spark
     * @return
     */
    private static Dataset<Row> getCleanedDataset(SparkSession spark) {
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .schema(BikeParquetSchema.schema())
                .option("timestampFormat", "dd/MM/yyyy HH:mm")
                .option("timeZone", "etc/UTC")
                .csv(file);

        data = data.filter(data.col("Duration").isNotNull()).filter(data.col("Duration").lt(TWELVE_HOURS_IN_SECONDS))
                .filter(data.col("Duration").gt(0));
        return data.filter(year(data.col("Start_Date")).geq(2011));
    }

    public static void repartitionWeekendPostParquet(SparkSession spark) {
        Dataset<Row> data = getCleanedDataset(spark);
        data = data.withColumn("Weekday", date_format(data.col("Start_Date"), "EEEE"));
        data = data.withColumn("isWeekend",
                data.col("Weekday").equalTo("Saturday")
                        .or(data.col("Weekday").equalTo("Sunday")));
        data.write().partitionBy("isWeekend")
                .parquet("cycle-data-results" + Time.now());
    }

    public static void repartitionWeekendPreParquet(SparkSession spark) {
        Dataset<Row> data = getCleanedDataset(spark);
        data = data.withColumn("Weekday", date_format(data.col("Start_Date"), "EEEE"));
        data = data.withColumn("isWeekend",
                data.col("Weekday").equalTo("Saturday")
                        .or(data.col("Weekday").equalTo("Sunday")));
        data.repartition(data.col("isWeekend")).write()
                .parquet("cycle-data-results" + Time.now());
    }

}
