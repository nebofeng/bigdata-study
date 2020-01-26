package pers.nebo.remotingdemo.server;
 
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
 
public class RmiServer {
 
    public static void main(String[] args) throws Exception {
        int port = 1099;
        // http://www.baidumcom/index.html
        //schema : rmi....reflection 
        String url = "rmi://localhost:1099/demo.zookeeper.remoting.server.HelloServiceImpl";
        LocateRegistry.createRegistry(port);
        Naming.rebind(url, new HelloServiceImpl());
    }
}