package com.nebo.hadoop.javaapi;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class Test {
	
	public static void main(String[] args) throws IOException, URISyntaxException{
		getHDFSNodes();
	}
	
	public static FileSystem getFileSystem() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();//读取配置文件
		
		 //FileSystem fs = FileSystem.get(conf);//获取文件系统对象
		//windows 上运行需要指定文件路径
		URI uri = new URI("hdfs://nebo:9000");
		FileSystem fs = FileSystem.get(uri, conf);
		
		return fs;
		
	}
	
	public static void mkdir() throws IOException, URISyntaxException{
		//获取文件系统
		FileSystem fs =getFileSystem();
		
		//创建文件目录
		fs.mkdirs(new Path("/nebo1/data"));
		fs.close();
		
	}
	
	public static void copyToHDFS() throws IOException, URISyntaxException{
		//第一步
		FileSystem fs =getFileSystem();
		
		Path sPath = new Path("D://Document/Bigdata/java.txt");
		
		Path dstPath = new Path("/nebo1");
		fs.copyFromLocalFile(sPath, dstPath);
		fs.close();
	}
	
	
	
	public static void getFile() throws IOException, URISyntaxException{
		FileSystem fs =getFileSystem();
		
		Path srcPath = new Path("/nebo1/java.txt");
		Path dstPath = new Path("D://Document");
		
		fs.copyToLocalFile(srcPath, dstPath);
		fs.close();
	}
	
	
	public static void ListAllFile () throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		
		FileStatus[] status = fs.listStatus(new Path("/nebo1"));
		Path[] listPaths = FileUtil.stat2Paths(status);
		
		for(Path p: listPaths){
			System.out.println(p);
		}
		fs.close();
	}
	
	//查看某个文件在fs集群中的位置
	public static void getFileLocal() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		
		Path path = new Path("/nebo1/java.txt");
		
		FileStatus fileStatus = fs.getFileStatus(path);
		
		BlockLocation[] blklocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		
		for (int i =0 ;i<blklocations.length ;i++){
			String[] hosts = blklocations[i].getHosts();
			System.out.println("block_ "+i+"_location:" +hosts[0]);
		}
		fs.close();
		
	}
	
	
	
	public static void rmdir() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		fs.delete(new Path("/nebo1/data"),true);
		
		
		fs.close();
	}
	//获取hdfs集群节点信息
	
	public static void getHDFSNodes() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		
		
		DistributedFileSystem hdfs = (DistributedFileSystem)fs ;
		
		DatanodeInfo[] dataNodeStatus = hdfs.getDataNodeStats();
		for(int i = 0;i<dataNodeStatus.length ;i++){
			System.out.println("DataNode_"+i+"_Name:"+dataNodeStatus[i].getHostName());
		}
		fs.close();
		
		
	}
}
