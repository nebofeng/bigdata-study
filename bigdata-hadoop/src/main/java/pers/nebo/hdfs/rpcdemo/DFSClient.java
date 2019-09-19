package pers.nebo.hdfs.rpcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/9/15
 * @ des : RPC的客户端
 */
public class DFSClient {
    //这是另外一个进程
    public static void main(String[] args) throws IOException {
       ClientProtocol nameNode= RPC.getProxy(
               ClientProtocol.class,
               1234L,
               new InetSocketAddress("localhost",9999),new Configuration());
       nameNode.mkdirPath("/data");
        System.out.println(nameNode.getFile(""));
    }

}
