package pers.nebo.hdfs.rpcdemo;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;


import java.io.IOException;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/9/14
 * @ des :
 */
public class NameNodeRpcServer implements ClientProtocol {
    @Override
    public void mkdirPath(String path) {
        System.out.println("服务器创建了目录： "+path);

    }

    @Override
    public String getFile(String Path) {
        return "hello word";
    }

    public static void main(String[] args) throws IOException {
        RPC.Server server=new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(ClientProtocol.class)
                .setInstance(new NameNodeRpcServer())
                .build();
        server.start();
        //服务启动之后，等待别人的调用
    }
}
