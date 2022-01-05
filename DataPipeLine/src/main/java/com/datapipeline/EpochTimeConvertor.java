package com.datapipeline;

import java.text.ParseException;


public class EpochTimeConvertor {

    public static long getEpochTime(String timeStamp) throws ParseException {
        long epochTime = 0;
        epochTime = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
                .parse(timeStamp.replace('-','/'))
                .getTime() / 1000;
        return epochTime ;
    }

    public static String getTimeStamp(long epochTime) throws ParseException {
        String timeStamp = null;
        timeStamp = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date(epochTime * 1000));
        return timeStamp;
    }
}
