package pers.nebo.zookeeper.lock;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/9/10
 * @ des : DistributedLock
 *
 */
public class DistributedLock implements Watcher {
    //定义会话的超时时间
    private final static int SESSION_TIME = 30000;

    private int threadId;
    private ZooKeeper zk = null;
    private String selfPath;
    private String waitPath;
    private String LOG_PREFIX_OF_THREAD;
    private static final int SESSION_TIMEOUT = 10000;
    private static final String GROUP_PATH = "/disLocks";
    private static final String SUB_PATH = "/disLocks/sub";
    private static final String CONNECTION_STRING = "node-01:2181,node-02:2181,node-03:2181";

    private static final int THREAD_NUM = 10;

    //确保连接zk成功；
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    //确保所有线程运行结束；semaphore信号
    private static final CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);

    private static final Logger LOG = Logger.getLogger(DistributedLock.class);

    //构造方法
    public DistributedLock(int id) {
        this.threadId = id;
        LOG_PREFIX_OF_THREAD = "【第" + threadId + "个线程】";
    }

    //程序入口
    public static void main(String[] args) {
        //
        BasicConfigurator.configure();
        for (int i = 0; i < THREAD_NUM; i++) {
            final int threadId = i + 1;
            new Thread() {
                @Override
                public void run() {
                    try {
                        DistributedLock dc = new DistributedLock(threadId);
                        //连接zookeeper集群
                        dc.createConnection(CONNECTION_STRING, SESSION_TIMEOUT);
                        System.out.println("3、在createConnection中，线程等待结束，向下执行");
                        //GROUP_PATH不存在的话，由一个线程创建即可；
                        synchronized (threadSemaphore) {
                            dc.createPath(GROUP_PATH, "该节点由线程" + threadId + "创建", true);
                        }
                        //获得锁
                        dc.getLock();
                    } catch (Exception e) {
                        LOG.error("【第" + threadId + "个线程】 抛出的异常：");
                        e.printStackTrace();
                    }
                }
            }.start();
        }
        try {
            threadSemaphore.await();
            LOG.info("所有线程运行结束!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取锁
     *
     * @return
     */
    private void getLock() throws KeeperException, InterruptedException {
        selfPath = zk.create(SUB_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        LOG.info(LOG_PREFIX_OF_THREAD + "创建锁路径:" + selfPath);

        if (checkMinPath()) {//判断当前线程，创建的临时节点，时候是编号最小的；如果是，表示此线程可获得锁
            getLockSuccess();
        }
    }

    /**
     * 创建节点
     *
     * @param path 节点path
     * @param data 初始数据内容
     * @return
     */
    public boolean createPath(String path, String data, boolean needWatch) throws KeeperException, InterruptedException {
        if (zk.exists(path, needWatch) == null) {
            LOG.warn(LOG_PREFIX_OF_THREAD + "节点创建成功, Path: "
                    + this.zk.create(path,
                    data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT)
                    + ", content: " + data);
        }
        return true;
    }

    /**
     * 创建ZK连接
     *
     * @param connectString  ZK服务器地址列表
     * @param sessionTimeout Session超时时间
     */
    public void createConnection(String connectString, int sessionTimeout) throws IOException, InterruptedException {
        zk = new ZooKeeper(connectString, sessionTimeout, this);
        System.out.println("1、创建连接，并等待");
        //CountDownLatch
        connectedSemaphore.await();
        System.out.println("2、创建连接后，等待结束；理应执行3、");
    }

    /**
     * 获取锁成功
     */
    public void getLockSuccess() throws KeeperException, InterruptedException {
        if (zk.exists(this.selfPath, false) == null) {
            LOG.error(LOG_PREFIX_OF_THREAD + "本节点已不在了...");
            return;
        }
        LOG.info(LOG_PREFIX_OF_THREAD + "获取锁成功，赶紧干活！");
        Thread.sleep(2000);
        LOG.info(LOG_PREFIX_OF_THREAD + "删除本节点：" + selfPath);
        //删除本节点
        zk.delete(this.selfPath, -1);
        //释放zk连接
        releaseConnection();
        //threadSemaphore数字递减；达到零后，让等待的线程继续执行
        threadSemaphore.countDown();
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
            }
        }
        LOG.info(LOG_PREFIX_OF_THREAD + "释放连接");
    }

    /**
     * 检查自己创建的临时节点是不是最小的节点
     *
     * @return
     */
    public boolean checkMinPath() throws KeeperException, InterruptedException {
        //获得所有子节点的路径
        List<String> subNodes = zk.getChildren(GROUP_PATH, false);
        //对子节点列表做排序
        Collections.sort(subNodes);

        //获得当前线程创建的临时节点，在子节点列表中排第几？
        int index = subNodes.indexOf(selfPath.substring(GROUP_PATH.length() + 1));
        switch (index) {
            case -1: {
                LOG.error(LOG_PREFIX_OF_THREAD + "本节点已不在了..." + selfPath);
                return false;
            }
            //当前线程创建的临时节点是最小的节点
            case 0: {
                LOG.info(LOG_PREFIX_OF_THREAD + "子节点中，我果然是老大" + selfPath);
                //获得锁
                return true;
            }
            default: {
                //找到比自己编号小，紧邻的临时节点
                this.waitPath = GROUP_PATH + "/" + subNodes.get(index - 1);
                LOG.info(LOG_PREFIX_OF_THREAD + "获取子节点中，排在我前面的" + waitPath);
                try {
                    //注册监听器
                    zk.getData(waitPath, true, new Stat());
                    //没有获得锁
                    return false;
                } catch (KeeperException e) {
                    if (zk.exists(waitPath, false) == null) {
                        LOG.info(LOG_PREFIX_OF_THREAD + "子节点中，排在我前面的" + waitPath + "已失踪，幸福来得太突然?");
                        //回调自己
                        return checkMinPath();
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    //回调函数
    public void process(WatchedEvent event) {
        if (event == null) {
            return;
        }
        Event.KeeperState keeperState = event.getState();
        Event.EventType eventType = event.getType();
        //Event.KeeperState.SyncConnected =>> The client is in the connected state 客户端处于连接状态
        if (Event.KeeperState.SyncConnected == keeperState) {
            if (Event.EventType.None == eventType) {//客户端连接上zkServer后，执行此分支
                LOG.info(LOG_PREFIX_OF_THREAD + "成功连接上ZK服务器");
                //connectedSemaphore数字递减；达到零后，让等待的线程继续执行
                System.out.println("4、createConnection后，客户端成功连接zkServer");
                connectedSemaphore.countDown();
                System.out.println("5、CountDownLatch: connectedSemaphore 递减为0；理应执行 -> 2、创建连接后，等待结束");
            } else if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                LOG.info(LOG_PREFIX_OF_THREAD + "收到情报，排我前面的家伙已挂，我是不是可以出山了？");
                try {
                    if (checkMinPath()) {
                        getLockSuccess();
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else if (Event.KeeperState.Disconnected == keeperState) {
            LOG.info(LOG_PREFIX_OF_THREAD + "与ZK服务器断开连接");
        } else if (Event.KeeperState.AuthFailed == keeperState) {
            LOG.info(LOG_PREFIX_OF_THREAD + "权限检查失败");
        } else if (Event.KeeperState.Expired == keeperState) {
            LOG.info(LOG_PREFIX_OF_THREAD + "会话失效");
        }
    }
}

