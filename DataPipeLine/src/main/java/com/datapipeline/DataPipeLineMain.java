package com.datapipeline;

import java.sql.SQLException;
import java.util.List;

public class DataPipeLineMain {
    public static String storage = null;
    public static void main(String[] args) {
        try {
            DbConnect.ConnectDb();
            List<String> queries = CreateTables.createTableQueries();
            for (String query : queries) {
                if (query.equalsIgnoreCase("HiveQuery")) {
                    storage = "Hive";
                    continue;
                }
                if (query.equalsIgnoreCase("MySqlQuery")) {
                    storage = "MySql";
                    continue;
                }
                QueryExecutor.executeQuery(query, "No", storage);
            }
            CreateInsertPartition.getSetPartitionStrtEndTime();

            do {
                CreateInsertPartition.doPartitioning();
            } while (CreateInsertPartition.maxRecTime >= CreateInsertPartition.partitonEndTime);

            GetSetMetaData.setMetaData();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DbConnect.hiveCon.close();
                DbConnect.mySqlCon.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}