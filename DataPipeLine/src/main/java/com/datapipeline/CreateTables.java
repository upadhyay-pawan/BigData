package com.datapipeline;

import java.util.ArrayList;
import java.util.List;

public class CreateTables {
    //private StringBuilder sqlQuery = new StringBuilder();
    public static List<String> createTableQueries() {
        List<String> queryList = new ArrayList<>();
        queryList.add("HiveQuery");
        queryList.add("Create database IF NOT EXISTS stockdb");
        queryList.add("Create table IF NOT EXISTS stockdb." + "ext_stocks" + "(record_id STRING,record_date STRING,create_time STRING,epoch_time STRING) row format delimited fields terminated by ',' stored as textfile tblproperties(\"skip.header.line.count\"=\"1\") ");
        queryList.add("Create table IF NOT EXISTS stockdb." + "us_stocks" + "(record_id STRING,record_date STRING,create_time STRING) partitioned by (epoch_time STRING)");
        queryList.add("load data LOCAL INPATH '/home/pavan/rawdata.csv'  OVERWRITE into table stockdb.ext_stocks");
        //queryList.add("load data LOCAL INPATH 'hdfs://172.28.35.110:50070/dataset/rawdata.csv'  OVERWRITE into table stockdb.ext_stocks");
        queryList.add("MySqlQuery");
        queryList.add("Create database IF NOT EXISTS stockdb");
        queryList.add("Create table IF NOT EXISTS stockdb." + "job_status" + " (job_name varchar(255),max_value varchar(255))");
        queryList.add("Create table IF NOT EXISTS stockdb." + "part_meta_data" + " (job_name varchar(255),last_partition_epochtime varchar(255),last_partition_end_epochtime varchar(255),last_partition_end_record_epochtime varchar(255))");
        return queryList;
    }
}

