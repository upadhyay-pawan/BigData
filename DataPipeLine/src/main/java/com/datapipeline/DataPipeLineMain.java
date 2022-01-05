package com.datapipeline;

import java.sql.SQLException;
import java.util.List;

public class DataPipeLineMain {

    public static void main(String[] args) {

        try {
            DbConnect dbConnect = new DbConnect();
            dbConnect.ConnectDb();
            CreateTables ct = new CreateTables();
            List<String> queries = ct.createTableQueries();
            for (String query : queries) {
                QueryExecutor.executeQuery(query, "No");
            }
            long partitonStartTime = 0;
            long partionIntervalTimeStamp = 0;
            long partitonEndTime = 0;
            boolean createPartition = false;
            String minMax = GetMetaInformation.getMinMaxFromTbl();
            String metaData = GetSetMetaData.getMetaData(HiveConstants.META_FILE_NAME);
            String[] tempArray1 = minMax.split("\\|");
            long maxRecTime = Long.parseLong(tempArray1[1]);
            long lastPartitionEpochTime = GetSetJobStatus.getJobStatus();
            if (metaData == null) {
                createPartition = true;
            } else {
                tempArray1 = metaData.split("\\|");
                String[] tempArray2 = tempArray1[0].split("\\=");
                // this is last partition key/name
                partionIntervalTimeStamp = Long.parseLong(tempArray2[1]);
                //this is last partition's end time and will become end time for next run
                tempArray2 = tempArray1[1].split("\\=");
                partitonEndTime = Long.parseLong(tempArray2[1]);
                //this is last partition's last record which will become start time for next run
                String[] tempArray3 = tempArray1[2].split("\\=");
                partitonStartTime = Long.parseLong(tempArray3[1]);
            }
            do {
                if (createPartition) {
                    if (partitonStartTime == 0) {
                        partitonStartTime = Long.parseLong(tempArray1[0]);
                        lastPartitionEpochTime = partitonStartTime;
                    } else {
                        lastPartitionEpochTime = partitonStartTime;
                        partitonStartTime = GetMetaInformation.getNextStartTimeFromTbl(partitonEndTime);
                    }
                    partitonEndTime = partitonStartTime + HiveConstants.PARTITION_FREQ;
                    partionIntervalTimeStamp = partitonStartTime;
                    CreateInsertPartition.createPartition(partionIntervalTimeStamp);
                    CreateInsertPartition.insertIntoPartition(partionIntervalTimeStamp, partitonStartTime, partitonEndTime);
//                    if (partitonEndTime >= maxRecTime) {
//                        GetSetJobStatus.setJobStatus(lastPartitionEpochTime);
//                    }
                }else {
                    CreateInsertPartition.insertIntoPartition(partionIntervalTimeStamp, partitonStartTime, partitonEndTime);
                    if (maxRecTime > partitonEndTime) {
                        createPartition = true;
                    }
                }
            } while (maxRecTime >= partitonEndTime);

            long lastPartitonRecEndTime = GetMetaInformation.getMetaInfoFromPart(partionIntervalTimeStamp);
            metaData = ("lastPartition.epochtime=".concat(String.valueOf(partionIntervalTimeStamp)))
                    .concat("|")
                    .concat("lastPartitionEnd.epochtime=".concat(String.valueOf(partitonEndTime)))
                    .concat("|")
                    .concat("lastPartitionEndRecord.epochtime=".concat(String.valueOf(lastPartitonRecEndTime)));
            GetSetMetaData.setMetaData(HiveConstants.META_FILE_NAME, metaData);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DbConnect.con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}