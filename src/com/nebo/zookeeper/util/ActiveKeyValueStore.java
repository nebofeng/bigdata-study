package com.nebo.zookeeper.util;

 

import java.nio.charset.Charset;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * 读写Zookeeper数据
 * @author dajiangtai
 *
 */
public class ActiveKeyValueStore extends ConnectionWatcher {
	private static final Charset CHARSET = Charset.forName("UTF-8");

	/**
	 * 注册永久节点
	 * @param path
	 * @param value
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void write(String path, String value) throws InterruptedException,
			KeeperException {
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			if(value == null){
				zk.create(path, null,
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}else{
				zk.create(path, value.getBytes(CHARSET),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
		} else {
			if(value == null){
				zk.setData(path, null, -1);
			}else{
				zk.setData(path, value.getBytes(CHARSET), -1);
			}
			
		}
	}
	
	/**
	 * 注册临时有序节点
	 * @param path
	 * @param value
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void writeEphemeralNode(String path, String value) throws InterruptedException,
			KeeperException {
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			if(value == null){
				zk.create(path, null,
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}else{
				zk.create(path, value.getBytes(CHARSET),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
			
		} else {
			if(value == null){
				zk.setData(path, null, -1);
			}else{
				zk.setData(path, value.getBytes(CHARSET), -1);
			}
			
		}
	}
	
	/**
	 * 注册临时有序节点
	 * @param path
	 * @param value
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void writeEPHEMERAL_SEQUENTIAL_Node(String path, String value) throws InterruptedException,
			KeeperException {
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			if(value == null){
				zk.create(path, null,
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			}else{
				zk.create(path, value.getBytes(CHARSET),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			}
			
		} else {
			if(value == null){
				zk.setData(path, null, -1);
			}else{
				zk.setData(path, value.getBytes(CHARSET), -1);
			}
			
		}
	}
	
	/**
	 * 注册持久有序节点
	 * @param path
	 * @param value
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void writePERSISTENT_SEQUENTIAL_Node(String path, String value) throws InterruptedException,
			KeeperException {
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			if(value == null){
				zk.create(path, null,
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			}else{
				zk.create(path, value.getBytes(CHARSET),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			}
			
		} else {
			if(value == null){
				zk.setData(path, null, -1);
			}else{
				zk.setData(path, value.getBytes(CHARSET), -1);
			}
			
		}
	}
	
	/**
	 * 判断节点是否存在
	 * @param path
	 * @param value
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void exists(String path, String value) throws KeeperException, InterruptedException{
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			if(value == null){
				zk.create(path, null,
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}else{
				zk.create(path, value.getBytes(CHARSET),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
		}
	}
	
	public void registerWatcher(String path ,Watcher watcher){
		try {
			zk.exists(path, watcher);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void delete(String path){
		try {
			zk.delete(path, -1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 读取节点数据
	 * @param path
	 * @param watcher
	 * @return
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public String read(String path, Watcher watcher)
			throws InterruptedException, KeeperException {
		byte[] data = zk.getData(path, watcher, null /* stat */);
		return new String(data, CHARSET);
	}
	
	/**
	 * 获取所有子节点
	 * @param path
	 * @param watcher
	 * @return
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public List<String> readChildren(String path, Watcher watcher)
			throws InterruptedException, KeeperException {
		List<String> childrens = null;
		if(watcher == null){
			childrens = zk.getChildren(path, false);
		}else{
			childrens = zk.getChildren(path, watcher, null);
		}
		return childrens;
	}
}