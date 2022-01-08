package com.datapipeline;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutor {

    public static ResultSet executeQuery(String query, String resultMode, String storage) throws SQLException{
        System.out.println(query);
        Statement stmt = null;
        ResultSet rs = null;
//        try {
            //System.out.println(query);
            if (storage.equalsIgnoreCase("MySql")) {
                stmt = DbConnect.mySqlCon.createStatement();
            } else {
                stmt = DbConnect.hiveCon.createStatement();
            }
            if (resultMode.equalsIgnoreCase("Yes")) {
                rs = stmt.executeQuery(query);

            } else {
                stmt.execute(query);
            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        return rs;
    }
}
