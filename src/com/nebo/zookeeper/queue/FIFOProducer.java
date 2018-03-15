package com.nebo.zookeeper.queue;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.KeeperException;

import com.nebo.zookeeper.util.ActiveKeyValueStore;
/**
 * 数据入队
 * @author dajiangtai
 *
 */
public class FIFOProducer {
	private ActiveKeyValueStore _store;
	public String rootNode = "/FIFO";// 代表根节点
	public static String queueNode = "/FIFO/q";// 子节点名称

	public FIFOProducer(String hosts) throws IOException, InterruptedException, KeeperException {
		_store = new ActiveKeyValueStore();
		// 连接Zookeeper
		_store.connect(hosts);
		// 判断根节点是否存在
		_store.exists(rootNode, null);
	}
	
	/**
	 * 插入节点
	 * @param data
	 */
	public void push(String data) {
		try {
			_store.writePERSISTENT_SEQUENTIAL_Node(queueNode, data);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	 public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		 String hosts = "192.168.8.130:2181";
		 final FIFOProducer fifoProducer = new FIFOProducer(hosts);
		 final Random _random = new Random();
		 for(int i=0; i<10; i++) {
			 final int j = 0;
	            new Thread() {
	                public void run() {
	                	fifoProducer.push(""+_random.nextInt(100));
	                };
	            }.start();
	        }
	 }
}
