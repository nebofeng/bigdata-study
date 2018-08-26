package com.nebo.hdfs_utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsUtils {
    static  FileSystem  fs;
   static {
       Configuration conf = new Configuration();  //获取当前的默认环境配
      // final String hdfsPath = "hdfs://139.199.172.112:9000";
       String nameservices = "mycluster";
       String[] namenodesAddr = {"hdfs://139.199.172.112:9000","hdfs://123.207.241.42:9000"};
       String[] namenodes = {"nn1","nn2"};
       conf.set("fs.defaultFS", "hdfs://" + nameservices);
       conf.set("dfs.nameservices",nameservices);
       conf.set("dfs.ha.namenodes." + nameservices, namenodes[0]+","+namenodes[1]);
       conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0]);
       conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1]);
       conf.set("dfs.client.failover.proxy.provider." + nameservices,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
       String hdfsRPCUrl = "hdfs://" + nameservices + ":" + 9000;


       try {
           fs = FileSystem.get(conf);

       } catch (IOException e) {
           e.printStackTrace();
       }
   }


    public static FSDataOutputStream   createFile(String path){
         Path dstPath = new Path(path);//目标路径
         FSDataOutputStream outputStream = null;
        try {
             if(fs.exists(dstPath)){
                 fs.delete(dstPath);
             }
            outputStream=   fs.create(dstPath);


        } catch (IOException e) {
            e.printStackTrace();

        }
        return outputStream;
    }


}
