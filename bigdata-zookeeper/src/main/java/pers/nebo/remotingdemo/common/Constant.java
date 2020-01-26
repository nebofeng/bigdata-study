package pers.nebo.remotingdemo.common;
 
public interface Constant {
 
    String ZK_CONNECTION_STRING = "192.168.133.19:2181,192.168.133.20:2181,192.168.133.21:2181";
    int ZK_SESSION_TIMEOUT = 5000;
    String ZK_REGISTRY_PATH = "/registry";  //must be created and persistent
    String ZK_PROVIDER_PATH = ZK_REGISTRY_PATH + "/provider";
}