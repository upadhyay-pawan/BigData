package com.datapipeline;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetSetJobStatus {
    public static long getJobStatus() throws SQLException {
        long maxValue = 0;
        String getQuery = "select max_value from stockdb.job_status where job_name='us_stocks_Load_job' ";
        ResultSet rs =  QueryExecutor.executeQuery(getQuery,"Yes","MySql");
        if (!rs.next()) {
            String insertQuery = "insert into stockdb.job_status values('us_stocks_Load_job',NULL)";
            QueryExecutor.executeQuery(insertQuery,"No","MySql");
        } else {
            maxValue = (rs.getString(1) == null?0: Long.parseLong(rs.getString(1)));
            System.out.println("maxvalue:" + maxValue);
        }
        return maxValue;
    }

    public static void setJobStatus(long lastCompletedPartitionEpochTime) throws Exception{
        String setQuery = "update stockdb.job_status set max_value=" + lastCompletedPartitionEpochTime + " where job_name='us_stocks_Load_job'";
        QueryExecutor.executeQuery(setQuery,"No","MySql");
    }

}
