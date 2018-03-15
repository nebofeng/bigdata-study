package com.nebo.zookeeper.queue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import com.nebo.zookeeper.util.ActiveKeyValueStore;
/**
 * 数据出队
 * @author dajiangtai
 *
 */
public class FIFOConsumer {
	private ActiveKeyValueStore _store;
	public String rootNode = "/FIFO";// 代表根节点
	public static String queueNode = "/FIFO/q";// 子节点名称
	
	public FIFOConsumer(String hosts) throws IOException, InterruptedException, KeeperException{
		_store = new ActiveKeyValueStore();
		// 连接Zookeeper
		_store.connect(hosts);
		// 判断根节点是否存在
		_store.exists(rootNode, null);
	}
	
	/**
	 * 消费数据
	 * @return
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	 public String pop() throws InterruptedException, KeeperException {
		 List<String> childrens = _store.readChildren(rootNode, null);
		 if(childrens.isEmpty()){
			 return null;
		 }
		 
		 Collections.sort(childrens);
		 
		 String firstChildren = rootNode+"/"+childrens.get(0);
		 String data = _store.read(firstChildren, null);
		 _store.delete(firstChildren);
		 return firstChildren;
	 }
	 
	 public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		 String hosts = "192.168.8.130:2181";
		 final FIFOConsumer fifoConsumer = new FIFOConsumer(hosts);
		 for(int i=0; i<10; i++) {
			 System.out.println("消费队列数据-"+fifoConsumer.pop());
		 }		
	 }
}
