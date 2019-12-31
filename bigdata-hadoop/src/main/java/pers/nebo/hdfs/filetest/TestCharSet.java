package pers.nebo.hdfs.filetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class TestCharSet {


    public static final String hdfsPath = "hdfs://10.0.0.200:8020";
    public static String  uploadPreHdfs = null;
    public static  String srcFolderNameSpace=null;
    static Configuration conf = new Configuration();
    static URI uri;
    static FileSystem fs;

    static {
        try {
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            uri = new URI(hdfsPath);
            fs = FileSystem.get(uri, conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        File file = new File("E:\\TestData\\我的.txt");
        String uploadPath ="/home/nebo/demo.txt";
        Path dstPath = new Path(hdfsPath+uploadPath);//目标路径
        try {
            FSDataOutputStream outputStream = fs.create(dstPath);
            FileInputStream  fr= new FileInputStream(file);
            BufferedReader bf = new BufferedReader(new InputStreamReader(fr,"GBK"));
            String line =null;
            while((line=bf.readLine())!=null){
                System.out.println(line+"====");
                 outputStream.write(line.getBytes("gbk"));
            }
            if(bf!=null){
                bf.close();
            }
            if(outputStream!=null){
                outputStream.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
