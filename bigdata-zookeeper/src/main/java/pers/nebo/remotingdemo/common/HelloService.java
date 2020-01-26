package pers.nebo.remotingdemo.common;
 
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *  继承 Remote
 */
public interface HelloService extends Remote {
 
    String sayHello(String name) throws RemoteException;
}