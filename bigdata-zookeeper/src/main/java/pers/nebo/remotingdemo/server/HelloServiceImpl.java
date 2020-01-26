package pers.nebo.remotingdemo.server;
 
 import pers.nebo.remotingdemo.common.HelloService;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
 
public class HelloServiceImpl extends UnicastRemoteObject implements HelloService {
 
    protected HelloServiceImpl() throws RemoteException {
    }
    @Override
    public String sayHello(String name) throws RemoteException {
    	System.out.println("server:  "+name);
        return String.format("Hello %s", name);
    }
}