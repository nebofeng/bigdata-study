package com.nebo.zookeeper.queue;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.nebo.zookeeper.util.ActiveKeyValueStore;
/**
 * 归入队列操作
 * @author dajiangtai
 *
 */
public class QueueClient {
	private ActiveKeyValueStore _store;
	public String rootNode = "/queue";// 代表根节点
	public static String queueNode = "/queue/q";//子节点名称
	private String startPath = "/queue/go";//队列满了的标识

	public QueueClient(String hosts) throws KeeperException,
			InterruptedException, IOException {
		_store = new ActiveKeyValueStore();
		// 连接Zookeeper
		_store.connect(hosts);
		// 判断根节点是否存在
		_store.exists(rootNode, null);
	}

	/**
	 * 加入队列
	 * 
	 * @param queueNode
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void joinQueue(String queueNode) throws InterruptedException,
			KeeperException {
		_store.writeEPHEMERAL_SEQUENTIAL_Node(queueNode, null);
		isFull();
	}

	/**
	 * 判断队列是否已满
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void isFull() throws InterruptedException, KeeperException {
		int queueSize = 3;
		int queueLength = _store.readChildren(rootNode, null).size();
		if (queueLength >= queueSize) {
			System.out.println("所有队员全部到达！");
			_store.writeEphemeralNode(startPath, null);
		}
	}

	
	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		String hosts = "192.168.8.130:2181";
		// 创建watcher
		QueueClient client = new QueueClient(hosts);
		Thread.sleep(10000);
		client.joinQueue(queueNode);
		client.joinQueue(queueNode);
		client.joinQueue(queueNode);
		Thread.sleep(60000);
	}
}
