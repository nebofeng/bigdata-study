package com.nebo.zookeeper.nameservice;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nebo.zookeeper.util.ActiveKeyValueStore;
/**
 * Zookeeper-统一命名服务
 * Watcher 监控
 * @author dajiangtai
 *
 */
public class ClientWatcher implements Watcher {
	private ActiveKeyValueStore _store;

	public ClientWatcher(String hosts) throws InterruptedException, IOException {
		_store = new ActiveKeyValueStore();
		//连接Zookeeper
		_store.connect(hosts);
	}

	/**
	 * 读取znode节点数据
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void displayConfig() throws InterruptedException, KeeperException {
		List<String> childrens = _store.readChildren(ClientRegister1.rootPATH, this);
		System.out.println("查询子节点信息");
		for(String children:childrens){
			String childrenPath = ClientRegister1.rootPATH+"/"+children;
			System.out.println(childrenPath+"节点的值为"+_store.read(childrenPath, this));
		}
		
	}

	/**
	 * 监控znode数据变化
	 */
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.NodeDataChanged) {
			try {
				displayConfig();
			} catch (InterruptedException e) {
				System.err.println("Interrupted. Exiting");
				Thread.currentThread().interrupt();
			} catch (KeeperException e) {
				System.err.printf("KeeperException: %s. Exiting.\n", e);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		String hosts="192.168.8.130:2181";
		//创建watcher
		ClientWatcher watcher = new ClientWatcher(hosts);
		//调用display方法
		watcher.displayConfig();
	}
}