package pers.nebo.remotingdemo.server;
 

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.nebo.remotingdemo.common.Constant;

public class ServiceProvider {
 
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProvider.class);
 

    private CountDownLatch latch = new CountDownLatch(1);
    
    /**
     * 发布RMI服务，注册RMI地址到Zookeeper中
     * @param remote HelloService对象
     * @param host 服务器主机
     * @param port 服务器端口
     */
    public void publish(Remote remote, String host, int port) {
        String url = publishService(remote, host, port); // 发布RMIF服务并返回rmi地址
        if (url != null) {
            ZooKeeper zk = connectServer(); // ���� ZooKeeper ����������ȡ ZooKeeper ����
            if (zk != null) {
                createNode(zk, url); // ���� ZNode ���� RMI ��ַ���� ZNode ��
            }
        }
    }
 
    // 发布RMI服务  注册端口 绑定对象
    private String publishService(Remote remote, String host, int port) {
        String url = null;
        try {
            url = String.format("rmi://%s:%d/%s", host, port, remote.getClass().getName());
            LocateRegistry.createRegistry(port);
            Naming.rebind(url, remote);
            LOGGER.debug("publish rmi service (url: {})", url);
        } catch (RemoteException | MalformedURLException e) {
            LOGGER.error("", e);
        }
        return url;
    }
 
    // 连接 ZooKeeper 服务器 集群
    private ZooKeeper connectServer() {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(Constant.ZK_CONNECTION_STRING, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown(); //
                    }
                }
            });
            latch.await(); // ʹ��ǰ�̴߳��ڵȴ�״̬
        } catch (IOException | InterruptedException e) {
            LOGGER.error("", e);
        }
        return zk;
    }
 


    // 带序列化的临时性节点
    private void createNode(ZooKeeper zk, String url) {
        try {
            byte[] data = url.getBytes(); //zookeeper 底层也是字节数组
            String path = zk.create(Constant.ZK_PROVIDER_PATH, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOGGER.debug("create zookeeper node ({} => {})", path, url);
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("", e);
        }
    }



    
}