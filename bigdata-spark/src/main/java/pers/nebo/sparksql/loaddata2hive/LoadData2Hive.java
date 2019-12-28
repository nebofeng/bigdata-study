package pers.nebo.sparksql.loaddata2hive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/12/27
 * @ des :
 */
public class LoadData2Hive {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("user.name", "hdfs");
        SparkConf conf=new SparkConf();
        conf.setMaster("local[2]");
        conf.set("yarn.resourcemanager.hostname","node1");


        SparkSession session = SparkSession.builder().appName("traffic2hive")
                .config(conf).enableHiveSupport().getOrCreate();
        session.sql("CREATE DATABASE IF NOT EXISTS traffic");
        session.sql("USE traffic");
        session.sql("DROP TABLE IF EXISTS monitor_flow_action");
        //在hive中创建monitor_flow_action表
        session.sql("CREATE TABLE IF NOT EXISTS monitor_flow_action "
                + "(date STRING,monitor_id STRING,camera_id STRING,car STRING,action_time STRING,speed STRING,road_id STRING,area_id STRING) "
                + "row format delimited fields terminated by '\t' ");
        session.sql("load data local inpath './monitor_flow_action' into table monitor_flow_action");

        //在hive中创建monitor_camera_info表
        session.sql("DROP TABLE IF EXISTS monitor_camera_info");
        session.sql("CREATE TABLE IF NOT EXISTS monitor_camera_info (monitor_id STRING, camera_id STRING) row format delimited fields terminated by '\t'");
        session.sql("LOAD DATA "
                + "LOCAL INPATH './monitor_camera_info'"
                + "INTO TABLE monitor_camera_info");
    }

}



