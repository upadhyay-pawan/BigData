package com.datapipeline;

import java.sql.SQLException;

public class CreateInsertPartition {
    public static long partitonStartTime = 0;
    public static long partionIntervalTimeStamp = 0;
    public static long partitonEndTime = 0;
    public static long maxRecTime = 0;
    public static long lastCompletedPartitionEpochTime = 0;
    public static boolean createPartition = false;
    public static String minMax = null;
    public static String metaData = null;
    public static String[] tempArray;


    public static void createPartition(long partitionEpochTime) throws Exception {
        String addPartQuery = "ALTER TABLE stockdb.us_stocks ADD IF NOT EXISTS PARTITION (epoch_time ='" + partitionEpochTime + "')";
        QueryExecutor.executeQuery(addPartQuery, "No", "Hive");
    }

    public static void insertIntoPartition(long partitionEpochTime, long startTime, long endTime) throws Exception {
        String insertPartQuery = "insert into stockdb.us_stocks PARTITION (epoch_time='" + partitionEpochTime + "') select record_id,record_date,create_time from stockdb.ext_stocks where epoch_time >=" + startTime + " and epoch_time <=" + endTime;
        QueryExecutor.executeQuery(insertPartQuery, "No", "Hive");
    }

    public static void getSetPartitionStrtEndTime() throws SQLException {
        minMax = GetMetaInformation.getMinMaxFromTbl();
        metaData = GetSetMetaData.getMetaData();
        tempArray = minMax.split("\\|");
        maxRecTime = Long.parseLong(tempArray[1]);
        lastCompletedPartitionEpochTime = GetSetJobStatus.getJobStatus();
        if (metaData == null) {
            createPartition = true;
        } else {
           tempArray = metaData.split("\\|");
            // this is last partition key/name
            partionIntervalTimeStamp = Long.parseLong(tempArray[0]);
            //this is last partition's end time and will become end time for next run
            partitonEndTime = Long.parseLong(tempArray[1]);
            //this is last partition's last record which will become start time for next run
            partitonStartTime = Long.parseLong(tempArray[2]);
        }
    }

    public static void doPartitioning() throws Exception {
        if (createPartition) {
            if (partitonStartTime == 0) {
                partitonStartTime = Long.parseLong(tempArray[0]);
                lastCompletedPartitionEpochTime = partitonStartTime;
            } else {
                lastCompletedPartitionEpochTime = partitonStartTime;
                partitonStartTime = GetMetaInformation.getNextStartTimeFromTbl(partitonEndTime);
            }
            partitonEndTime = partitonStartTime + HiveConstants.PARTITION_FREQ;
            partionIntervalTimeStamp = partitonStartTime;
            createPartition(partionIntervalTimeStamp);
            insertIntoPartition(partionIntervalTimeStamp, partitonStartTime, partitonEndTime);
            if (partitonEndTime >= maxRecTime) {
                GetSetJobStatus.setJobStatus(lastCompletedPartitionEpochTime);
            }
        } else {
            insertIntoPartition(partionIntervalTimeStamp, partitonStartTime, partitonEndTime);
            if (maxRecTime > partitonEndTime) {
                createPartition = true;
            }
        }
    }


}
