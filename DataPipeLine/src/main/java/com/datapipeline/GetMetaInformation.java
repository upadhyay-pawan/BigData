package com.datapipeline;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetMetaInformation {
    public static String getMinMaxFromTbl() throws SQLException {
        String metaInfo = null;
        String minQuery = "select min(epoch_time),max(epoch_time) from stockdb.ext_stocks";
        ResultSet rs = QueryExecutor.executeQuery(minQuery, "Yes","Hive");
        int columns = rs
                .getMetaData()
                .getColumnCount();
        System.out.println(columns);
        while (rs.next()) {
            System.out.println();
        }
        String recStartTime = rs.getString(1);
        String recEndTime = rs.getString(2);
        metaInfo = String
                .valueOf(recStartTime)
                .concat("|")
                .concat(String.valueOf(recEndTime));
        System.out.println("metaInfo:" + metaInfo);
        return metaInfo;
    }

    public static long getMetaInfoFromPart(long partitionEpochTime) throws SQLException {
        String getQuery = "select epoch_time from stockdb.ext_stocks  where record_id=(select max(cast(record_id as int)) from stockdb.us_stocks  where epoch_time='" + partitionEpochTime + "')";
        ResultSet rs = QueryExecutor.executeQuery(getQuery, "Yes","Hive");
        if (rs.next()) {
            int columns = rs
                    .getMetaData()
                    .getColumnCount();
            while (rs.next()) {
                System.out.println();
            }
            long partitionLastRecordTime = Long.parseLong((rs.getString(1)));
            long metaInfo = partitionLastRecordTime;
            return metaInfo;
        } else {
            System.out.println("no more records to add in this partition");
            return 0;
        }
    }

    public static long getNextStartTimeFromTbl(long lastEpochTime) throws SQLException {
        String nextRecQuery = "select epoch_time from stockdb.ext_stocks where epoch_time>" + lastEpochTime + " limit 1";
        ResultSet rs = QueryExecutor.executeQuery(nextRecQuery, "Yes","Hive");
        int columns = rs
                .getMetaData()
                .getColumnCount();
        System.out.println(columns);
        while (rs.next()) {
            System.out.println();
        }
        long nextStartTime = Long.parseLong(rs.getString(1));
        return nextStartTime;
    }
}
