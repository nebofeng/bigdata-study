package com.nebo.homework.mergefiles;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.nebo.hadoop.javaapi.RegexAcceptPathFilter;

public class UploadMergeFiles {
	private static FileSystem fs = null;
	private static FileSystem local = null;
	
	public static void main(String[] args) throws   IOException, URISyntaxException {
		uploadAndMergeList (new Path("D://Document/Bigdata/homework/冯小波合并小文件作业/data/*"), 
				                    new Path("hdfs://nebo:9000/nebo1/upload/") );
		
	} 
	
	public static void uploadAndMergeList( Path srcPath ,Path dstPath) throws IOException, URISyntaxException {
		Configuration conf =new Configuration();
		
		 URI uri = new URI("hdfs://nebo:9000");
		
		  fs = FileSystem.get(uri, conf);
		 
		 local =fs.getLocal(conf);
		 
		 FileStatus[] dfileStatus = local.globStatus(srcPath, new RegexExcludePathFilter("^.*vpn$"));
		 
		 Path[] dirList = FileUtil.stat2Paths(dfileStatus);
		 
		 FSDataOutputStream out = null;
		 for(Path p : dirList){
			 String namePath = p.toString();
			 int i = namePath.lastIndexOf("/");
			 String uploadName = namePath.substring(i+1, namePath.length());
			 out = fs.create(new Path(dstPath+"/"+uploadName+".txt"));
			 FileStatus[] ffileStatus=local.globStatus(new Path(p+"/*"), new RegexAcceptPathFilter("^.*txt$"));
			 Path[] file = FileUtil.stat2Paths(ffileStatus);
			 FSDataInputStream in = null;
			 for(Path pf: file){
//				 System.out.println(pf);
				 
			      in = local.open(pf);
				  IOUtils.copyBytes(in, out, 4096, false) ;
			 }
			 
		 }
		 fs.close();
		 
	 	
		
	}

}
