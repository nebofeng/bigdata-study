package com.nebo.zookeeper.clustermanage;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

import com.nebo.zookeeper.util.ActiveKeyValueStore;
/**
 * 注册第一个临时节点
 * @author dajiangtai
 *
 */
public class ClientRegister1 {
	public static final String rootPATH = "/GroupMembers";//根节点
	public static final String nodePATH = "regionServer1";//临时节点
	private ActiveKeyValueStore _store;
	private Random _random = new Random();

	public ClientRegister1(String hosts) throws IOException,
			InterruptedException, KeeperException {
		_store = new ActiveKeyValueStore();// 定义一个类
		_store.connect(hosts);// 连接Zookeeper
		_store.exists(rootPATH, null);//判断根节点是否存在
		_store.writeEphemeralNode(rootPATH+"/"+nodePATH, null);//注册临时节点
	}

	public void run() throws InterruptedException, KeeperException {		
		//维持临时节点一直存在
		while (true) {
			System.out.println(nodePATH);
			TimeUnit.SECONDS.sleep(_random.nextInt(10));
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		String hosts = "192.168.8.130:2181";
		ClientRegister1 client = new ClientRegister1(hosts);
		client.run();
	}
}
