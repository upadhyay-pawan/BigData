package com.datapipeline;

public class CreateInsertPartition {

    public static void createPartition(long partitionEpochTime) {
        String addPartQuery = "ALTER TABLE stockdb.us_stocks ADD IF NOT EXISTS PARTITION (epoch_time ='" + partitionEpochTime + "')";
        QueryExecutor.executeQuery(addPartQuery, "No");
    }

    public static void insertIntoPartition(long partitionEpochTime, long startTime, long endTime) {
        String insertPartQuery = "insert into stockdb.us_stocks PARTITION (epoch_time='" + partitionEpochTime + "') select record_id,record_date,create_time from stockdb.ext_stocks where epoch_time >=" + startTime + " and epoch_time <=" + endTime;
        QueryExecutor.executeQuery(insertPartQuery, "No");
    }
}
