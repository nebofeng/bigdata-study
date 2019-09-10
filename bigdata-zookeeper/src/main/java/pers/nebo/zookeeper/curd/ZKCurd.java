package pers.nebo.zookeeper.curd;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.SettingUtil;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/9/10
 * @ des : zk 原生的curd 操作
 *
 */
public class ZKCurd {
    //定义会话的超时时间
    private final static int SESSION_TIME=30000;
    //定义zookee集群的ip地址
    private final static String ZK_SERVERS= SettingUtil.getKey("setting.properties","zookeeper_server");

    private static final Logger LOGGER=Logger.getLogger(ZKCurd.class);

    //定义zookeeper实例

    static ZooKeeper zk =null;

    //创建监视器

    Watcher watcher=new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            LOGGER.info("event : "+watchedEvent.toString());
        }
    };

    //初始化zk连接
    @Before
    public void connect() throws Exception{
        zk=new ZooKeeper(ZK_SERVERS,SESSION_TIME,this.watcher);
        long sessionId=zk.getSessionId();
        LOGGER.info("sessionId:"+sessionId);
    }

    @After
    public  void close() throws Exception{
        zk.close();
    }

    @Test
    public void createPersistentNode() throws Exception{
        //1.创建持久化节点
        String result=zk.create("/test1" ,"test1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //2. 查看节点是否创建成功
        LOGGER.info("create result: "+result);
        LOGGER.info("查看zk_test节点是否存在： "+zk.getData("/zk_test",false,null
        ));
    }

    @Test
    public void testDeleteZnode() throws Exception {
        zk.delete("/kkb", -1);
        if (null == zk.exists("/kkb", false)) {
            System.out.println("节点删除成功!");
        }
    }

    @Test
    public void createEphemeralNode() {
        try {
            //创建临时节点
            zk.create("/tmp", "tmpdata".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //休眠20秒；根据是否休眠，命令行端观察现象
            Thread.sleep(20000);
            //查看节点时候创建成功
            LOGGER.info("/tmp是否存在：" + zk.exists("/tmp", null));
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 判断制定节点是否存在
     *
     * @throws Exception
     */
    @Test
    public void exist() throws Exception {
        Stat stat = zk.exists("/kkb", null);
        if (stat != null) {
            System.out.println(stat.getVersion());
            LOGGER.info("node exists!");
        } else {
            LOGGER.info("node not exists!");
        }
    }

    @Test
    public void testGetData() throws KeeperException, InterruptedException {
        byte[] data = zk.getData("/test", null, null);
        System.out.println("data is :" + new String(data));
    }

    @Test
    public void testGetData1() throws KeeperException, InterruptedException {
        //获得/kkb节点的数据；观察连接的zkServer是哪个？
        byte[] data = zk.getData("/kkb", null, null);
        System.out.println("data is :" + new String(data));

        Thread.sleep(10000);
        //在休眠期间，关闭连接的zkServer

        //问：能否再次获得/kkb的数据？
        byte[] data1 = zk.getData("/kkb", null, null);
        System.out.println("data is :" + new String(data1));
    }

    @Test
    public void testGetDataWatch() throws Exception{
        byte[] data = zk.getData("/kkb", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                LOGGER.info("===>>> event type: " + event.getType());
                System.out.println("triger watcher!");
            }
        }, null);
        System.out.println("------------->>> data is " + new String(data));

        //观察现象
        System.out.println("the first time to set data");
        zk.setData("/kkb", "kkbdata2".getBytes(), -1);

        //观察现象
        System.out.println("the second time to set data");
        zk.setData("/kkb", "kkbdata3".getBytes(), -1);

    }

    private void setData(String path, byte[] data) throws Exception {
        Stat stat = zk.setData(path, data, -1);
        if (null != stat) {
            System.out.println("内容设置成功!");
        }
    }





}
