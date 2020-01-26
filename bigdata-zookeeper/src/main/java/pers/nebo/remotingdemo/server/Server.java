package pers.nebo.remotingdemo.server;


import pers.nebo.remotingdemo.common.HelloService;

public class Server {
 
    public static void main(String[] args) throws Exception {


        String host = "localhost";//虚拟网卡可以和linux通讯的地址
        int port = Integer.parseInt("11214");//每启动一个server，端口号递增，手动启动
        ServiceProvider provider = new ServiceProvider();


        HelloService helloService =   new HelloServiceImpl();
        provider.publish(helloService, host, port);
        Thread.sleep(Long.MAX_VALUE);
    }
}