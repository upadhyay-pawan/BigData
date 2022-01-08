package com.datapipeline;

import java.sql.ResultSet;
import java.sql.SQLException;


public class GetSetMetaData {
    public static long partionIntervalTimeStamp = 0;
    public static long partitonEndTime = 0;
    public static long lastPartitonRecEndTime = 0;

    public static String getMetaData() throws SQLException {
        String metaData = null;
        String getQuery = "select * from stockdb.part_meta_data where job_name='us_stocks_Load_job' ";
        ResultSet rs = QueryExecutor.executeQuery(getQuery, "Yes", "MySql");
        if (!rs.next()) {
            String insertQuery = "insert into stockdb.part_meta_data values('us_stocks_Load_job',NULL,NULL,NULL)";
            QueryExecutor.executeQuery(insertQuery, "No", "MySql");
            metaData=null;
        } else {
            partionIntervalTimeStamp = (rs.getString(2) == null ? 0 : Long.parseLong(rs.getString(2)));
            partitonEndTime = (rs.getString(3) == null ? 0 : Long.parseLong(rs.getString(3)));
            lastPartitonRecEndTime = (rs.getString(4) == null ? 0 : Long.parseLong(rs.getString(4)));
            metaData = (String.valueOf(partionIntervalTimeStamp))
                    .concat("|")
                    .concat(String.valueOf(partitonEndTime))
                    .concat("|")
                    .concat(String.valueOf(lastPartitonRecEndTime));

        }
        return metaData;
    }

    public static void setMetaData() throws SQLException {
        long lastPartitonRecEndTime = GetMetaInformation.getMetaInfoFromPart(CreateInsertPartition.partionIntervalTimeStamp);
        String updateQuery = "update stockdb.part_meta_data set last_partition_epochtime='" + CreateInsertPartition.partionIntervalTimeStamp + "',last_partition_end_epochtime='" + CreateInsertPartition.partitonEndTime + "',last_partition_end_record_epochtime='" + lastPartitonRecEndTime + "' where job_name='us_stocks_Load_job'";
        QueryExecutor.executeQuery(updateQuery, "No", "MySql");
    }
}
