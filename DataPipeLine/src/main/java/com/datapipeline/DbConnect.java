package com.datapipeline;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DbConnect {

    public static Connection hiveCon = null;
    public static Connection mySqlCon = null;
    public static void ConnectDb() {

        try {
            /* Step 01 :Loading Drivers */
            Class.forName(HiveConstants.DRIVER_NAME);
            System.out.println("Hive Driver Loaded successfully");

            // Step 02 : Obtaining Connection Object
            hiveCon = DriverManager.getConnection(
                    HiveConstants.CONNECTION_URL,
                    HiveConstants.USERNAME,
                    HiveConstants.PASSWORD);
            System.out.println("Obtained Connection Object for Hive");

            // Step 03 : Obtaining Statement object
            // Obtaining the Statement Object
           // Statement stmt = con.createStatement();

            //My sql connection
            Class.forName(MysqlConstants.DRIVER_NAME);
            mySqlCon = DriverManager.getConnection(MysqlConstants.CONNECTION_URL, MysqlConstants.USERNAME, MysqlConstants.PASSWORD);
            System.out.println("Obtained Connection Object for Mysql");

        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found");
        } catch (SQLException e) {
            System.out.println("Problem Connecting DB");
            e.printStackTrace();
        }

    }
}
