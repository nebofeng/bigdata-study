package com.nebo.zookeeper.clustermanage;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.KeeperException;

import com.nebo.zookeeper.util.ActiveKeyValueStore;
public class ClientRegister3 {
	public static final String rootPATH = "/GroupMembers";//根节点
	public static final String nodePATH = "regionServer3";//临时节点
	private ActiveKeyValueStore _store;
	private Random _random = new Random();

	public ClientRegister3(String hosts) throws IOException,
			InterruptedException, KeeperException {
		_store = new ActiveKeyValueStore();// 定义一个类
		_store.connect(hosts);// 连接Zookeeper
		_store.exists(rootPATH, null);		
		_store.writeEphemeralNode(rootPATH+"/"+nodePATH, null);
	}

	public void run() throws InterruptedException, KeeperException {		
		while (true) {
			System.out.println(nodePATH);
			TimeUnit.SECONDS.sleep(_random.nextInt(10));
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		String hosts = "192.168.8.130:2181";
		ClientRegister3 client = new ClientRegister3(hosts);
		client.run();
	}
}
