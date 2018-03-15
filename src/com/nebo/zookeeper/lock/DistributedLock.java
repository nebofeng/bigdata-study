package com.nebo.zookeeper.lock;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

/**
 * 实现分布式锁
 * @author dajiangtai
 *
 */
public class DistributedLock implements Lock, Watcher{
    private ZooKeeper zk;
    private String root = "/mylocks";//根
    private String lockName;//竞争资源的标志
    private String waitNode;//等待前一个锁
    public String myZnode;//当前锁
    private CountDownLatch latch;//计数器
    private CountDownLatch connectedSignal=new CountDownLatch(1);
    private int sessionTimeout = 30000; 
    /**
     * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
     * @param config 192.168.8.130:2181
     * @param lockName 竞争资源标志,lockName中不能包含单词_lock_
     */
    public DistributedLock(String config, String lockName){
        this.lockName = lockName;
        // 创建一个与服务器的连接
         try {
            zk = new ZooKeeper(config, sessionTimeout, this);
            connectedSignal.await();//等待Zookeeper连接成功，取消等待
            Stat stat = zk.exists(root, false);
            if(stat == null){
                // 创建根节点
                zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
            }
        } catch (IOException e) {
        	throw new LockException(e);
        } catch (KeeperException e) {
        	throw new LockException(e);
        } catch (InterruptedException e) {
        	throw new LockException(e);
        }
    }

    /**
     * zookeeper节点的监视器的执行方法
     */
    public void process(WatchedEvent event) {
    	System.out.println("------------触发开始----------------");
    	//注册监视，一旦zk建立连接，触发次方法
    	//释放connectedSignal等待，然后执行其他代码
    	if(event.getState()==KeeperState.SyncConnected){
    		connectedSignal.countDown();
    	}
    	System.out.println("this.latch="+this.latch);
    	//注册监视，一旦监控到节点消失，释放latch等待
        if(this.latch != null) {  
        	System.out.println(myZnode+"节点解锁！！！！！！！！！！！！！！！！！");
            this.latch.countDown();  
        }
        
        System.out.println("------------触发结束----------------");
    }
    //获取锁方法
    public void lock() {   
        try {
            if(this.tryLock()){
            	//当前节点获得锁权限，继续往后执行业务代码
                System.out.println("当前节点" +myZnode + "获得锁权限！");
                return ;
            }else{
            	//当前节点未获得锁权限，等待上一个节点解锁，然后再往后执行业务代码
                waitForLock(waitNode, sessionTimeout);//等待锁
            }
        } catch (KeeperException e) {
            throw new LockException(e);
        } catch (InterruptedException e) {
            throw new LockException(e);
        } 
    }

    //尝试获取锁权限
    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            if(lockName.contains(splitStr))
                throw new LockException("lockName can not contains \\u000B");
            //创建临时子节点
            myZnode = zk.create(root + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            //取出所有子节点
            List<String> subNodes = zk.getChildren(root, false);
            //取出所有lockName的锁
            List<String> lockObjNodes = new ArrayList<String>();
            for (String node : subNodes) {
                String _node = node.split(splitStr)[0];
                if(_node.equals(lockName)){
                    lockObjNodes.add(node);
                }
            }
            Collections.sort(lockObjNodes);
          
            if(myZnode.equals(root+"/"+lockObjNodes.get(0))){
                //如果是最小的节点,则表示取得锁
            	//System.out.println(myZnode + "==" + lockObjNodes.get(0));
                return true;
            }
            //如果不是最小的节点，找到比自己小1的节点
            String subMyZnode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
            waitNode = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyZnode) - 1);//找到前一个子节点
        } catch (KeeperException e) {
            throw new LockException(e);
        } catch (InterruptedException e) {
            throw new LockException(e);
        }
        return false;
    }

    public boolean tryLock(long time, TimeUnit unit) {
        try {
            if(this.tryLock()){
                return true;
            }
            return waitForLock(waitNode,time);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 未获得锁权限的节点，注册监听上一个获得锁的节点，一旦上一个节点解锁
     * 该节点取消等待，返回true，继续往后执行业务代码
     * @param lower
     * @param waitTime
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    
    private boolean waitForLock(String lower, long waitTime) throws InterruptedException, KeeperException {
        Stat stat = zk.exists(root + "/" + lower,this);//同时注册监听。
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
        if(stat != null){
        	System.out.println(myZnode+ "等待接替" + root + "/" + lower+"节点的锁服务");
        	//构造CountDownLatch
        	this.latch = new CountDownLatch(1);
        	//等待，这里应该一直等待其他线程释放锁
        	//一旦有临时节点消失，会触发process方法，latch减一取消等待，返回往后执行其他业务代码
        	this.latch.await();
        	this.latch = null;
        }
        return true;
    }

    /**
     * 解锁释放临时节点
     */
    public void unlock() {
        try {
            System.out.println("unlock " + myZnode);
            zk.delete(myZnode,-1);
            myZnode = null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }

    public Condition newCondition() {
        return null;
    }
    
    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }

}