package com.nebo.zookeeper.configmanage;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nebo.zookeeper.util.ActiveKeyValueStore;
/**
 * Zookeeper-配置管理
 * Watcher 监控
 * @author dajiangtai
 *
 */
public class ConfigWatcher implements Watcher {
	private ActiveKeyValueStore _store;

	public ConfigWatcher(String hosts) throws InterruptedException, IOException {
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
		String value = _store.read(ConfigUpdater.PATH, this);
		System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
	}

	/**
	 * 监控znode数据变化
	 */
	public void process(WatchedEvent event) {
		System.out.printf("Process incoming event: %s\n", event.toString());
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
		ConfigWatcher watcher = new ConfigWatcher(hosts);
		//调用display方法
		watcher.displayConfig();
		//然后一直处于监控状态
		Thread.sleep(Long.MAX_VALUE);
	}
}