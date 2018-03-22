package com.scottlogic.blog.analysis;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A new schema is required as Parquet columns can't contain spaces
 */
public class BikeParquetSchema {

    public static StructType schema() {
        StructType schema = new StructType(new StructField[]{
                new StructField("Rental_Id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("Duration", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("Bike_Id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("End_Date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("EndStation_Id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("EndStation_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Start_Date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("StartStation_Id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("StartStation_Name", DataTypes.StringType, true, Metadata.empty())
        });
        return schema;
    }

}
