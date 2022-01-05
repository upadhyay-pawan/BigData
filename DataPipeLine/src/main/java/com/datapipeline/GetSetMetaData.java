package com.datapipeline;

import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.util.Scanner;

public class GetSetMetaData {
    public static String getMetaData(String filename) {
        String metaData = null;
        try {
            File myObj = new File(filename);
            if (myObj.exists()) {
                Scanner myReader = new Scanner(myObj);
                while (myReader.hasNextLine()) {
                    metaData = myReader.nextLine();
                    System.out.println(metaData);
                }
                myReader.close();
            }
        } catch (IOException e) {
            System.out.println("An error occurred in getMetaData");
            e.printStackTrace();
        }
        return metaData;
    }

    public static void setMetaData(String filename, String metaData) {
        try {
            File myObj = new File(filename);
            if (myObj.exists()) {
                myObj.delete();
            }
            myObj.createNewFile();
            FileWriter myWriter = new FileWriter(filename);
            myWriter.write(metaData);
            myWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred in setMetaData.");
            e.printStackTrace();
        }
    }
}
